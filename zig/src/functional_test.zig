// functional_test.zig — End-to-end functional tests for the Zig datanode.
//
// Starts a real TCP server with a real disk and partition, then exercises
// the full packet lifecycle over TCP: create extent → write → read →
// stream read → mark delete → get watermarks → get partition size →
// heartbeat, etc.
//
// Uses page_allocator for server infrastructure (intentionally long-lived)
// and testing.allocator for client-side packet allocations (to catch leaks).

const std = @import("std");
const net = std.net;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const codec = @import("codec.zig");
const handler_mod = @import("handler.zig");
const connection_mod = @import("connection.zig");
const space_mgr_mod = @import("space_manager.zig");
const disk_mod = @import("disk.zig");
const partition_mod = @import("partition.zig");
const replication_mod = @import("replication.zig");
const master_client_mod = @import("master_client.zig");
const crc32_mod = @import("crc32.zig");
const json_mod = @import("json.zig");
const Packet = pkt_mod.Packet;
const ServerInner = handler_mod.ServerInner;
const SpaceManager = space_mgr_mod.SpaceManager;
const Disk = disk_mod.Disk;
const DataPartition = partition_mod.DataPartition;
const ReplicationPipeline = replication_mod.ReplicationPipeline;
const MasterClient = master_client_mod.MasterClient;

// ──────────────────────────────────────────────────────────────────
// Test Harness
// ──────────────────────────────────────────────────────────────────

const TEST_PARTITION_ID: u64 = 9999;

/// Short read timeout (1 second) for test connections so background threads exit quickly.
const TEST_READ_TIMEOUT_SECS: u32 = 1;

/// Test-specific connection handler that uses a short read timeout for fast cleanup.
fn testHandleConnection(stream: net.Stream, server: *ServerInner) void {
    connection_mod.handleConnectionTimeout(stream, server, server_alloc, TEST_READ_TIMEOUT_SECS);
}

// Page allocator for server-side infrastructure (long-lived, no leak check).
const server_alloc = std.heap.page_allocator;

// Atomic counter for unique test directory names.
var test_dir_counter = std.atomic.Value(u32).init(0);

/// Complete test harness: disk, partition, space manager, server, TCP listener.
/// Heap-allocated so pointers remain stable for background threads.
const TestHarness = struct {
    tcp_server: net.Server,
    server_port: u16,
    server_thread: ?std.Thread,
    inner: *HarnessInner,
    shutdown: std.atomic.Value(bool),
    tmp_dir: [128]u8,
    tmp_dir_len: usize,

    const HarnessInner = struct {
        space_mgr: SpaceManager,
        master_client: MasterClient,
        replication: ReplicationPipeline,
        server: ServerInner,
    };

    fn setup() !*TestHarness {
        // Unique directory per test invocation
        const dir_id = test_dir_counter.fetchAdd(1, .monotonic);

        var tmp_buf: [128]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&tmp_buf, "/tmp/cubefs_zig_ft_{d}", .{dir_id}) catch return error.NameTooLong;
        const tmp_len = tmp_path.len;

        std.fs.cwd().deleteTree(tmp_path) catch {};
        std.fs.cwd().makePath(tmp_path) catch {};

        var disk_buf: [192]u8 = undefined;
        const disk_path = std.fmt.bufPrint(&disk_buf, "{s}/disk0", .{tmp_path}) catch return error.NameTooLong;
        std.fs.cwd().makePath(disk_path) catch {};

        var dp_buf: [256]u8 = undefined;
        const dp_path_str = std.fmt.bufPrintZ(&dp_buf, "{s}/disk0/datapartition_{d}", .{ tmp_path, TEST_PARTITION_ID }) catch return error.NameTooLong;
        std.fs.cwd().makePath(dp_path_str) catch {};

        // Heap-allocate paths for stable pointers
        const stable_dp_path = try server_alloc.dupeZ(u8, dp_path_str);
        const stable_disk_path = try server_alloc.dupe(u8, disk_path);

        const d = try Disk.init(server_alloc, stable_disk_path, 0, 0, 0);

        const dp = try DataPartition.init(
            server_alloc,
            TEST_PARTITION_ID,
            1,
            stable_dp_path,
            &.{},
            d,
        );

        const inner = try server_alloc.create(HarnessInner);
        inner.* = .{
            .space_mgr = SpaceManager.init(server_alloc),
            .master_client = MasterClient.init(server_alloc, &.{}, "127.0.0.1", 0, "test", 17330, 17340),
            .replication = ReplicationPipeline.init(server_alloc),
            .server = undefined,
        };

        try inner.space_mgr.addDisk(d);
        inner.space_mgr.addPartition(dp);

        inner.replication.start();

        inner.server = ServerInner.init(
            server_alloc,
            &inner.space_mgr,
            &inner.master_client,
            &inner.replication,
            null,
            "127.0.0.1",
            0,
        );

        const address = try net.Address.parseIp4("127.0.0.1", 0);
        var tcp_server = try address.listen(.{ .reuse_address = true });
        const port = tcp_server.listen_address.getPort();

        const harness = try server_alloc.create(TestHarness);
        harness.* = .{
            .tcp_server = tcp_server,
            .server_port = port,
            .server_thread = null,
            .inner = inner,
            .shutdown = std.atomic.Value(bool).init(false),
            .tmp_dir = undefined,
            .tmp_dir_len = tmp_len,
        };
        @memcpy(harness.tmp_dir[0..tmp_len], tmp_path);

        harness.server_thread = try std.Thread.spawn(.{}, acceptLoop, .{ &harness.tcp_server, &inner.server, &harness.shutdown });

        std.time.sleep(5 * std.time.ns_per_ms);

        return harness;
    }

    fn teardown(self: *TestHarness) void {
        // Signal shutdown
        self.shutdown.store(true, .release);
        // Close listening socket to break the accept loop
        self.tcp_server.deinit();
        // Detach accept thread (will return on socket close error)
        if (self.server_thread) |t| t.detach();
        // Stop replication pipeline (joins all 16 worker threads)
        self.inner.replication.deinit();
        // Pause for in-flight connection threads to finish
        std.time.sleep(100 * std.time.ns_per_ms);
        // Clean up temp directory
        const dir_name = self.tmp_dir[0..self.tmp_dir_len];
        std.fs.cwd().deleteTree(dir_name) catch {};
    }

    fn acceptLoop(tcp_server: *net.Server, server: *ServerInner, shutdown: *std.atomic.Value(bool)) void {
        while (!shutdown.load(.acquire)) {
            const conn = tcp_server.accept() catch return;
            _ = std.Thread.spawn(.{}, testHandleConnection, .{
                conn.stream,
                server,
            }) catch {
                conn.stream.close();
                continue;
            };
        }
    }
};

/// Connect a client to the test server.
fn connectClient(port: u16) !net.Stream {
    const address = try net.Address.parseIp4("127.0.0.1", port);
    return try net.tcpConnectToAddress(address);
}

/// Send a packet and receive the response.
fn roundtrip(stream: net.Stream, pkt: *const Packet, allocator: Allocator) !Packet {
    try codec.writePacket(stream.writer(), pkt);
    return try codec.readPacket(stream.reader(), allocator);
}

var next_req_id: i64 = 1;
fn nextReqId() i64 {
    const id = next_req_id;
    next_req_id += 1;
    return id;
}

// ──────────────────────────────────────────────────────────────────
// Functional Tests
// ──────────────────────────────────────────────────────────────────

test "functional: create extent" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{
        .opcode = proto.OP_CREATE_EXTENT,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = 1024,
        .req_id = nextReqId(),
    };

    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_OK, resp.result_code);
    try testing.expectEqual(@as(u64, 1024), resp.extent_id);
    try testing.expectEqual(req.req_id, resp.req_id);
}

test "functional: create extent with auto-allocate ID" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{
        .opcode = proto.OP_CREATE_EXTENT,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = 0,
        .req_id = nextReqId(),
    };

    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_OK, resp.result_code);
    try testing.expect(resp.extent_id >= 1024); // MIN_EXTENT_ID
}

test "functional: write then read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create extent
    {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2000,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Write data
    const write_data = "Hello, CubeFS Zig DataNode!";
    const crc = crc32_mod.hash(write_data);
    {
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2000,
            .extent_offset = 0,
            .size = @intCast(write_data.len),
            .crc = crc,
            .data = write_data,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqual(crc, resp.crc);
    }

    // Read data back
    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2000,
            .extent_offset = 0,
            .size = @intCast(write_data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqual(@as(u32, @intCast(write_data.len)), resp.size);
        try testing.expectEqualStrings(write_data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

test "functional: stream read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create + write
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 2001, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "Stream read test data payload";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2001,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Stream read
    {
        const req = Packet{
            .opcode = proto.OP_STREAM_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2001,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

test "functional: random write then read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 2002, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "random write at offset 4096!";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{
            .opcode = proto.OP_RANDOM_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2002,
            .extent_offset = 4096,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2002,
            .extent_offset = 4096,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
    }
}

test "functional: mark delete then read fails" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 2003, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Mark delete
    {
        const req = Packet{ .opcode = proto.OP_MARK_DELETE, .partition_id = TEST_PARTITION_ID, .extent_id = 2003, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Read should fail
    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2003,
            .extent_offset = 0,
            .size = 100,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_NOT_EXIST_ERR, resp.result_code);
    }
}

test "functional: get watermarks" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create two extents
    for ([_]u64{ 3000, 3001 }) |eid| {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = eid, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Write to first
    const data = "watermark test data";
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 3000, .extent_offset = 0, .size = @intCast(data.len), .crc = crc32_mod.hash(data), .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Get watermarks — should return JSON array
    {
        const req = Packet{ .opcode = proto.OP_GET_ALL_WATERMARKS, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expect(resp.size > 0);
        try testing.expect(resp.data.len > 2);
    }
}

test "functional: get applied id" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{ .opcode = proto.OP_GET_APPLIED_ID, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_OK, resp.result_code);
    try testing.expectEqual(@as(u32, 8), resp.size);
    const applied = std.mem.readInt(u64, resp.data[0..8], .big);
    try testing.expectEqual(@as(u64, 0), applied);
}

test "functional: get partition size" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create + write
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 4000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "partition size test";
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 4000, .extent_offset = 0, .size = @intCast(data.len), .crc = crc32_mod.hash(data), .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    {
        const req = Packet{ .opcode = proto.OP_GET_PARTITION_SIZE, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqual(@as(u32, 16), resp.size);
        const used_size = std.mem.readInt(u64, resp.data[0..8], .big);
        const extent_count = std.mem.readInt(u64, resp.data[8..16], .big);
        try testing.expect(used_size >= data.len);
        try testing.expect(extent_count >= 1);
    }
}

test "functional: heartbeat" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{ .opcode = proto.OP_DATA_NODE_HEARTBEAT, .partition_id = 0, .req_id = nextReqId() };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    // Heartbeat handler now returns okReply immediately — actual response
    // is sent asynchronously via HTTP POST to /dataNode/response
    try testing.expectEqual(proto.OP_OK, resp.result_code);
    try testing.expectEqual(@as(u32, 0), resp.size);
}

test "functional: create data partition via AdminTask in pkt.data" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Build AdminTask JSON for CreateDataPartition
    const admin_task_json =
        \\{"ID":"test-task-1","OpCode":96,"Request":{"PartitionId":50001,"VolumeId":"testvol","PartitionSize":137438953472,"Hosts":["127.0.0.1:17310"],"ReplicaNum":1}}
    ;

    var req = Packet{
        .opcode = proto.OP_CREATE_DATA_PARTITION,
        .partition_id = 0,
        .req_id = nextReqId(),
        .data = admin_task_json,
        .size = @intCast(admin_task_json.len),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_OK, resp.result_code);
    // Response should contain the disk path
    try testing.expect(resp.size > 0);

    // Verify partition was created by reading from it
    const read_req = Packet{
        .opcode = proto.OP_GET_PARTITION_SIZE,
        .partition_id = 50001,
        .req_id = nextReqId(),
    };
    var read_resp = try roundtrip(stream, &read_req, allocator);
    defer read_resp.deinit();
    try testing.expectEqual(proto.OP_OK, read_resp.result_code);
}

test "functional: create data partition with empty data returns error" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Empty data should return ARG_MISMATCH_ERR
    const req = Packet{
        .opcode = proto.OP_CREATE_DATA_PARTITION,
        .partition_id = 0,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_ARG_MISMATCH_ERR, resp.result_code);
}

test "functional: delete data partition via AdminTask in pkt.data" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // First create a partition
    const create_json =
        \\{"ID":"test-task-2","OpCode":96,"Request":{"PartitionId":50002,"VolumeId":"testvol","PartitionSize":1073741824,"Hosts":["127.0.0.1:17310"],"ReplicaNum":1}}
    ;
    {
        var req = Packet{
            .opcode = proto.OP_CREATE_DATA_PARTITION,
            .partition_id = 0,
            .req_id = nextReqId(),
            .data = create_json,
            .size = @intCast(create_json.len),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Now delete it via AdminTask in data field
    const delete_json =
        \\{"ID":"test-task-3","OpCode":97,"Request":{"PartitionId":50002}}
    ;
    {
        var req = Packet{
            .opcode = proto.OP_DELETE_DATA_PARTITION,
            .partition_id = 0,
            .req_id = nextReqId(),
            .data = delete_json,
            .size = @intCast(delete_json.len),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }
}

test "functional: load data partition via AdminTask in pkt.data" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Load existing test partition using AdminTask in data field
    const load_json = std.fmt.comptimePrint(
        "{{\"ID\":\"test-task-4\",\"OpCode\":98,\"Request\":{{\"PartitionId\":{d}}}}}",
        .{TEST_PARTITION_ID},
    );
    var req = Packet{
        .opcode = proto.OP_LOAD_DATA_PARTITION,
        .partition_id = 0, // intentionally 0 — should use data field
        .req_id = nextReqId(),
        .data = load_json,
        .size = @intCast(load_json.len),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_OK, resp.result_code);
    // Should return JSON watermarks array
    try testing.expect(resp.size > 0);
}

test "functional: partition not found" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{
        .opcode = proto.OP_READ,
        .result_code = proto.OP_INIT_RESULT_CODE,
        .partition_id = 99999,
        .extent_id = 1,
        .size = 100,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_NOT_EXIST_ERR, resp.result_code);
}

test "functional: extent not found" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{
        .opcode = proto.OP_READ,
        .result_code = proto.OP_INIT_RESULT_CODE,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = 99999,
        .size = 100,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_NOT_EXIST_ERR, resp.result_code);
}

test "functional: duplicate extent create" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 5000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 5000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_EXIST_ERR, resp.result_code);
    }
}

test "functional: multiple ops on single connection" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // 1. Create
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 6000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // 2. Write 1 KB
    var write_buf: [1024]u8 = undefined;
    for (&write_buf, 0..) |*b, i| b.* = @truncate(i);
    const crc = crc32_mod.hash(&write_buf);
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 6000, .extent_offset = 0, .size = 1024, .crc = crc, .data = &write_buf, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // 3. Read 1 KB back
    {
        const req = Packet{ .opcode = proto.OP_READ, .result_code = proto.OP_INIT_RESULT_CODE, .partition_id = TEST_PARTITION_ID, .extent_id = 6000, .extent_offset = 0, .size = 1024, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqual(@as(u32, 1024), resp.size);
        try testing.expectEqualSlices(u8, &write_buf, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }

    // 4. Partition size
    {
        const req = Packet{ .opcode = proto.OP_GET_PARTITION_SIZE, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // 5. Watermarks
    {
        const req = Packet{ .opcode = proto.OP_GET_ALL_WATERMARKS, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }
}

test "functional: large write and read 64KB" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 7000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data_size: usize = 64 * 1024;
    const data = try allocator.alloc(u8, data_size);
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 131 +% 17);
    const crc = crc32_mod.hash(data);

    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 7000, .extent_offset = 0, .size = @intCast(data_size), .crc = crc, .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    {
        const req = Packet{ .opcode = proto.OP_READ, .result_code = proto.OP_INIT_RESULT_CODE, .partition_id = TEST_PARTITION_ID, .extent_id = 7000, .extent_offset = 0, .size = @intCast(data_size), .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqual(@as(u32, @intCast(data_size)), resp.size);
        try testing.expectEqualSlices(u8, data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

test "functional: concurrent connections" {
    var h = try TestHarness.setup();
    defer h.teardown();

    var threads: [4]?std.Thread = .{null} ** 4;
    var results: [4]bool = .{false} ** 4;

    for (0..4) |i| {
        const ctx = ConcurrentCtx{ .port = h.server_port, .extent_base = @intCast(8000 + i * 100), .result = &results[i] };
        threads[i] = std.Thread.spawn(.{}, concurrentWorker, .{ctx}) catch null;
    }

    for (0..4) |i| {
        if (threads[i]) |t| t.join();
        try testing.expect(results[i]);
    }
}

const ConcurrentCtx = struct { port: u16, extent_base: u64, result: *bool };

fn concurrentWorker(ctx: ConcurrentCtx) void {
    const allocator = std.heap.page_allocator;
    const stream = connectClient(ctx.port) catch return;
    defer stream.close();

    // Create
    var create_req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = ctx.extent_base, .req_id = @intCast(ctx.extent_base) };
    var create_resp = roundtrip(stream, &create_req, allocator) catch return;
    defer create_resp.deinit();
    if (create_resp.result_code != proto.OP_OK) return;

    // Write
    const data = "concurrent test data";
    var write_req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = ctx.extent_base, .extent_offset = 0, .size = @intCast(data.len), .crc = crc32_mod.hash(data), .data = data, .req_id = @intCast(ctx.extent_base + 1) };
    var write_resp = roundtrip(stream, &write_req, allocator) catch return;
    defer write_resp.deinit();
    if (write_resp.result_code != proto.OP_OK) return;

    // Read
    var read_req = Packet{ .opcode = proto.OP_READ, .result_code = proto.OP_INIT_RESULT_CODE, .partition_id = TEST_PARTITION_ID, .extent_id = ctx.extent_base, .extent_offset = 0, .size = @intCast(data.len), .req_id = @intCast(ctx.extent_base + 2) };
    var read_resp = roundtrip(stream, &read_req, allocator) catch return;
    defer read_resp.deinit();
    if (read_resp.result_code != proto.OP_OK) return;
    if (!std.mem.eql(u8, data, read_resp.data)) return;

    ctx.result.* = true;
}

test "functional: unknown opcode" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{ .opcode = 0xEE, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_ERR, resp.result_code);
}

test "functional: write without data returns error" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 9000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 9000, .extent_offset = 0, .size = 0, .req_id = nextReqId() };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_ARG_MISMATCH_ERR, resp.result_code);
}

test "functional: broadcast min applied id" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    var arg_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &arg_buf, 42, .big);

    const req = Packet{ .opcode = proto.OP_BROADCAST_MIN_APPLIED_ID, .partition_id = TEST_PARTITION_ID, .arg_len = 8, .arg = &arg_buf, .req_id = nextReqId() };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_OK, resp.result_code);

    // Verify
    const get_req = Packet{ .opcode = proto.OP_GET_APPLIED_ID, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
    var get_resp = try roundtrip(stream, &get_req, allocator);
    defer get_resp.deinit();
    try testing.expectEqual(proto.OP_OK, get_resp.result_code);
    const applied = std.mem.readInt(u64, get_resp.data[0..8], .big);
    try testing.expectEqual(@as(u64, 42), applied);
}

test "functional: stub opcodes return ok" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // These opcodes dispatch through withPartition but return ok for valid partitions
    const partition_opcodes = [_]u8{
        proto.OP_DECOMMISSION_DATA_PARTITION,
        proto.OP_BATCH_UNLOCK_NORMAL_EXTENT,
        proto.OP_NOTIFY_REPLICAS_TO_REPAIR,
        proto.OP_STOP_DATA_PARTITION_REPAIR,
    };

    for (partition_opcodes) |opcode| {
        const req = Packet{ .opcode = opcode, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // QoS with empty arg returns ok
    {
        const req = Packet{ .opcode = proto.OP_QOS, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }
}

test "functional: get max extent id and partition size" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    for ([_]u64{ 9200, 9201, 9202 }) |eid| {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = eid, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const req = Packet{ .opcode = proto.OP_GET_MAX_EXTENT_ID_AND_PARTITION_SIZE, .partition_id = TEST_PARTITION_ID, .req_id = nextReqId() };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_OK, resp.result_code);
    try testing.expectEqual(@as(u32, 24), resp.size);
    const max_id = std.mem.readInt(u64, resp.data[0..8], .big);
    try testing.expect(max_id >= 9202);
}

// ──────────────────────────────────────────────────────────────────
// Repair Read Tests
// ──────────────────────────────────────────────────────────────────

test "functional: extent repair read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create and write data
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "repair read test data payload";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 10000, .extent_offset = 0, .size = @intCast(data.len), .crc = crc, .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Repair read — same as stream read, returns data with CRC
    {
        const req = Packet{
            .opcode = proto.OP_EXTENT_REPAIR_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10000,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

test "functional: tiny extent repair read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10001, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "tiny repair test";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 10001, .extent_offset = 0, .size = @intCast(data.len), .crc = crc, .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    {
        const req = Packet{
            .opcode = proto.OP_TINY_EXTENT_REPAIR_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10001,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

test "functional: snapshot extent repair read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10002, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "snapshot repair test data";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 10002, .extent_offset = 0, .size = @intCast(data.len), .crc = crc, .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    {
        const req = Packet{
            .opcode = proto.OP_SNAPSHOT_EXTENT_REPAIR_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10002,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

// ──────────────────────────────────────────────────────────────────
// Backup Read/Write Tests
// ──────────────────────────────────────────────────────────────────

test "functional: backup read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10100, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "backup read test data payload";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 10100, .extent_offset = 0, .size = @intCast(data.len), .crc = crc, .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Backup read returns data with CRC (same as stream read)
    {
        const req = Packet{
            .opcode = proto.OP_BACKUP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10100,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

test "functional: backup write" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10101, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Backup write (random write, always sync)
    const data = "backup write sync data";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{
            .opcode = proto.OP_BACKUP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10101,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Read back to verify
    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10101,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
    }
}

// ──────────────────────────────────────────────────────────────────
// Batch Lock/Unlock Tests
// ──────────────────────────────────────────────────────────────────

test "functional: batch lock and unlock extents" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create some extents
    for ([_]u64{ 10200, 10201, 10202 }) |eid| {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = eid, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Batch lock with JSON arg
    const lock_json = "[{\"ExtentId\":10200,\"GcFlag\":1},{\"ExtentId\":10201,\"GcFlag\":2}]";
    {
        const req = Packet{
            .opcode = proto.OP_BATCH_LOCK_NORMAL_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .arg_len = @intCast(lock_json.len),
            .arg = lock_json,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Batch unlock
    {
        const req = Packet{
            .opcode = proto.OP_BATCH_UNLOCK_NORMAL_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }
}

test "functional: batch lock with invalid json returns error" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const bad_json = "not valid json";
    const req = Packet{
        .opcode = proto.OP_BATCH_LOCK_NORMAL_EXTENT,
        .partition_id = TEST_PARTITION_ID,
        .arg_len = @intCast(bad_json.len),
        .arg = bad_json,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_ARG_MISMATCH_ERR, resp.result_code);
}

// ──────────────────────────────────────────────────────────────────
// Decommission Tests
// ──────────────────────────────────────────────────────────────────

test "functional: decommission partition" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Decommission — sets partition to read-only
    {
        const req = Packet{
            .opcode = proto.OP_DECOMMISSION_DATA_PARTITION,
            .partition_id = TEST_PARTITION_ID,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // After decommission, reads still work
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10300, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        // ExtentStore still accepts creates (decommission is partition-level metadata)
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }
}

// ──────────────────────────────────────────────────────────────────
// QoS Tests
// ──────────────────────────────────────────────────────────────────

test "functional: qos with json config" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const qos_json = "{\"DiskReadIops\":5000,\"DiskWriteIops\":3000,\"DiskReadFlow\":104857600,\"DiskWriteFlow\":52428800}";
    const req = Packet{
        .opcode = proto.OP_QOS,
        .partition_id = TEST_PARTITION_ID,
        .arg_len = @intCast(qos_json.len),
        .arg = qos_json,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_OK, resp.result_code);
}

test "functional: qos with invalid json returns error" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const bad_json = "{broken json";
    const req = Packet{
        .opcode = proto.OP_QOS,
        .partition_id = TEST_PARTITION_ID,
        .arg_len = @intCast(bad_json.len),
        .arg = bad_json,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_ARG_MISMATCH_ERR, resp.result_code);
}

// ──────────────────────────────────────────────────────────────────
// Heartbeat with QoS Tests
// ──────────────────────────────────────────────────────────────────

test "functional: heartbeat with qos config" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Heartbeat with QoS enabled in the arg
    const hb_arg = "{\"QosEnable\":true,\"DiskReadIops\":5000,\"DiskWriteIops\":3000}";
    const req = Packet{
        .opcode = proto.OP_DATA_NODE_HEARTBEAT,
        .partition_id = 0,
        .arg_len = @intCast(hb_arg.len),
        .arg = hb_arg,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    // Heartbeat handler returns okReply immediately — QoS is parsed from arg
    try testing.expectEqual(proto.OP_OK, resp.result_code);
    try testing.expectEqual(@as(u32, 0), resp.size);
}

test "functional: heartbeat response contains partition reports" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create an extent and write data so the partition has content
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10400, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "heartbeat partition report test";
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 10400, .extent_offset = 0, .size = @intCast(data.len), .crc = crc32_mod.hash(data), .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Heartbeat handler now returns okReply immediately — actual heartbeat
    // data with partition reports is sent via HTTP POST to /dataNode/response
    const req = Packet{ .opcode = proto.OP_DATA_NODE_HEARTBEAT, .partition_id = 0, .req_id = nextReqId() };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    try testing.expectEqual(proto.OP_OK, resp.result_code);
    try testing.expectEqual(@as(u32, 0), resp.size);
}

// ──────────────────────────────────────────────────────────────────
// Load Data Partition Tests
// ──────────────────────────────────────────────────────────────────

test "functional: load data partition returns watermarks" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create and write to establish watermarks
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10500, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "load partition test";
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 10500, .extent_offset = 0, .size = @intCast(data.len), .crc = crc32_mod.hash(data), .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Load partition — returns JSON watermarks
    {
        const req = Packet{
            .opcode = proto.OP_LOAD_DATA_PARTITION,
            .partition_id = TEST_PARTITION_ID,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expect(resp.size > 0);
        try testing.expect(resp.data.len > 2); // valid JSON array
    }
}

test "functional: load nonexistent partition returns error" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    const req = Packet{
        .opcode = proto.OP_LOAD_DATA_PARTITION,
        .partition_id = 99999,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_NOT_EXIST_ERR, resp.result_code);
}

// ──────────────────────────────────────────────────────────────────
// Read Tiny Delete Record Tests
// ──────────────────────────────────────────────────────────────────

test "functional: read tiny delete records" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Request tiny delete records — should return ok (even if empty)
    const req = Packet{
        .opcode = proto.OP_READ_TINY_DELETE_RECORD,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();
    try testing.expectEqual(proto.OP_OK, resp.result_code);
}

// ──────────────────────────────────────────────────────────────────
// Follower Read Tests
// ──────────────────────────────────────────────────────────────────

test "functional: stream follower read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10600, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "follower read test";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{ .opcode = proto.OP_WRITE, .partition_id = TEST_PARTITION_ID, .extent_id = 10600, .extent_offset = 0, .size = @intCast(data.len), .crc = crc, .data = data, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Stream follower read — dispatched to same handler as stream read
    {
        const req = Packet{
            .opcode = proto.OP_STREAM_FOLLOWER_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10600,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

// ──────────────────────────────────────────────────────────────────
// Sync Write Tests
// ──────────────────────────────────────────────────────────────────

test "functional: sync write then read" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 10700, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    const data = "sync write data with fsync";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{
            .opcode = proto.OP_SYNC_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10700,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Read back
    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 10700,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
    }
}

// ──────────────────────────────────────────────────────────────────
// Replication Tests — Multi-Node Harness
// ──────────────────────────────────────────────────────────────────

/// A multi-node test harness: starts N independent server instances,
/// each with its own disk, partition, and TCP port, sharing the same
/// partition ID so replication can be tested across nodes.
const ReplicationTestHarness = struct {
    nodes: [MAX_NODES]*TestNode,
    node_count: usize,

    const MAX_NODES = 3;

    const TestNode = struct {
        tcp_server: net.Server,
        server_port: u16,
        server_thread: ?std.Thread,
        inner: *TestNodeInner,
        shutdown: std.atomic.Value(bool),
        node_idx: usize,

        const TestNodeInner = struct {
            space_mgr: SpaceManager,
            master_client: MasterClient,
            replication: ReplicationPipeline,
            server: ServerInner,
        };
    };

    fn setup(node_count: usize) !*ReplicationTestHarness {
        std.debug.assert(node_count <= MAX_NODES);

        const harness = try server_alloc.create(ReplicationTestHarness);
        harness.node_count = node_count;

        for (0..node_count) |i| {
            harness.nodes[i] = try setupNode(i);
        }

        // Give all servers time to be ready
        std.time.sleep(10 * std.time.ns_per_ms);

        return harness;
    }

    fn setupNode(idx: usize) !*TestNode {
        // Each node gets its own directory
        var path_buf: [256]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&path_buf, "/tmp/cubefs_zig_repltest_{d}", .{idx}) catch unreachable;

        // Clean up first
        std.fs.cwd().deleteTree(tmp_path) catch {};
        std.fs.cwd().makePath(tmp_path) catch {};

        var disk_path_buf: [256]u8 = undefined;
        const disk_path = std.fmt.bufPrint(&disk_path_buf, "/tmp/cubefs_zig_repltest_{d}/disk0", .{idx}) catch unreachable;
        std.fs.cwd().makePath(disk_path) catch {};

        var dp_path_buf: [256]u8 = undefined;
        const dp_path_str = std.fmt.bufPrintZ(&dp_path_buf, "/tmp/cubefs_zig_repltest_{d}/disk0/datapartition_{d}", .{ idx, TEST_PARTITION_ID }) catch unreachable;
        std.fs.cwd().makePath(dp_path_str) catch {};

        // Copy the path to stable heap memory
        const stable_dp_path = try server_alloc.dupeZ(u8, dp_path_str);

        const d = try Disk.init(server_alloc, disk_path, 0, 0, 0);

        const dp = try DataPartition.init(
            server_alloc,
            TEST_PARTITION_ID,
            1,
            stable_dp_path,
            &.{},
            d,
        );

        const inner = try server_alloc.create(TestNode.TestNodeInner);
        inner.* = .{
            .space_mgr = SpaceManager.init(server_alloc),
            .master_client = MasterClient.init(server_alloc, &.{}, "127.0.0.1", 0, "test", 17330, 17340),
            .replication = ReplicationPipeline.init(server_alloc),
            .server = undefined,
        };

        try inner.space_mgr.addDisk(d);
        inner.space_mgr.addPartition(dp);
        inner.replication.start();

        inner.server = ServerInner.init(
            server_alloc,
            &inner.space_mgr,
            &inner.master_client,
            &inner.replication,
            null,
            "127.0.0.1",
            0,
        );

        const address = try net.Address.parseIp4("127.0.0.1", 0);
        var tcp_server = try address.listen(.{ .reuse_address = true });
        const port = tcp_server.listen_address.getPort();

        const node = try server_alloc.create(TestNode);
        node.* = .{
            .tcp_server = tcp_server,
            .server_port = port,
            .server_thread = null,
            .inner = inner,
            .shutdown = std.atomic.Value(bool).init(false),
            .node_idx = idx,
        };

        node.server_thread = try std.Thread.spawn(.{}, nodeAcceptLoop, .{ &node.tcp_server, &inner.server, &node.shutdown });

        return node;
    }

    fn nodeAcceptLoop(tcp_server: *net.Server, server: *ServerInner, shutdown: *std.atomic.Value(bool)) void {
        while (!shutdown.load(.acquire)) {
            const conn = tcp_server.accept() catch return;
            _ = std.Thread.spawn(.{}, testHandleConnection, .{
                conn.stream,
                server,
            }) catch {
                conn.stream.close();
                continue;
            };
        }
    }

    fn teardown(self: *ReplicationTestHarness) void {
        for (0..self.node_count) |i| {
            const node = self.nodes[i];
            node.shutdown.store(true, .release);
            node.tcp_server.deinit();
            if (node.server_thread) |t| t.detach();
            node.inner.replication.deinit();
        }
        std.time.sleep(100 * std.time.ns_per_ms);
        for (0..self.node_count) |i| {
            var path_buf: [256]u8 = undefined;
            const tmp_path = std.fmt.bufPrint(&path_buf, "/tmp/cubefs_zig_repltest_{d}", .{i}) catch continue;
            std.fs.cwd().deleteTree(tmp_path) catch {};
        }
    }

    fn portStr(port: u16, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "127.0.0.1:{d}", .{port}) catch "127.0.0.1:0";
    }
};

test "functional: replication — write to leader forwards to follower" {
    const allocator = testing.allocator;
    var rh = try ReplicationTestHarness.setup(2);
    defer rh.teardown();

    const leader_port = rh.nodes[0].server_port;
    const follower_port = rh.nodes[1].server_port;

    // Create extent on both nodes
    for ([_]u16{ leader_port, follower_port }) |port| {
        const stream = try connectClient(port);
        defer stream.close();
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 20000, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Write to leader with replication arg pointing to follower
    var addr_buf: [64]u8 = undefined;
    const follower_addr = std.fmt.bufPrint(&addr_buf, "/127.0.0.1:{d}", .{follower_port}) catch unreachable;

    const data = "replicated write data for follower";
    const crc = crc32_mod.hash(data);

    {
        const leader_stream = try connectClient(leader_port);
        defer leader_stream.close();
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20000,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .remaining_followers = 1,
            .arg_len = @intCast(follower_addr.len),
            .arg = follower_addr,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(leader_stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Give replication pipeline time to forward
    std.time.sleep(50 * std.time.ns_per_ms);

    // Read from follower to verify data was replicated
    {
        const follower_stream = try connectClient(follower_port);
        defer follower_stream.close();
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20000,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(follower_stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
        try testing.expectEqual(crc, resp.crc);
    }
}

test "functional: replication — chain replication A->B->C" {
    const allocator = testing.allocator;
    var rh = try ReplicationTestHarness.setup(3);
    defer rh.teardown();

    const port_a = rh.nodes[0].server_port;
    const port_b = rh.nodes[1].server_port;
    const port_c = rh.nodes[2].server_port;

    // Create extent on all 3 nodes
    for ([_]u16{ port_a, port_b, port_c }) |port| {
        const stream = try connectClient(port);
        defer stream.close();
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 20100, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Write to A with remaining_followers=2 and arg="/B/C"
    var addr_buf: [128]u8 = undefined;
    const follower_addrs = std.fmt.bufPrint(&addr_buf, "/127.0.0.1:{d}/127.0.0.1:{d}", .{ port_b, port_c }) catch unreachable;

    const data = "chain replication A->B->C test data";
    const crc = crc32_mod.hash(data);

    {
        const stream_a = try connectClient(port_a);
        defer stream_a.close();
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20100,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .remaining_followers = 2,
            .arg_len = @intCast(follower_addrs.len),
            .arg = follower_addrs,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream_a, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Give chain replication time to propagate
    std.time.sleep(100 * std.time.ns_per_ms);

    // Verify data on Node B
    {
        const stream_b = try connectClient(port_b);
        defer stream_b.close();
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20100,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream_b, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
    }

    // Verify data on Node C
    {
        const stream_c = try connectClient(port_c);
        defer stream_c.close();
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20100,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream_c, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
    }
}

test "functional: replication — write succeeds locally even if follower unreachable" {
    const allocator = testing.allocator;
    var h = try TestHarness.setup();
    defer h.teardown();

    const stream = try connectClient(h.server_port);
    defer stream.close();

    // Create extent
    {
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 20200, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Write with follower pointing to a non-existent port
    const data = "write with unreachable follower";
    const crc = crc32_mod.hash(data);
    const bad_follower = "/127.0.0.1:1"; // port 1 is not listening
    {
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20200,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .remaining_followers = 1,
            .arg_len = @intCast(bad_follower.len),
            .arg = bad_follower,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        // Leader's local write should still succeed
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Verify data is written locally
    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20200,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
    }
}

test "functional: replication — replicated random write" {
    const allocator = testing.allocator;
    var rh = try ReplicationTestHarness.setup(2);
    defer rh.teardown();

    const leader_port = rh.nodes[0].server_port;
    const follower_port = rh.nodes[1].server_port;

    // Create extent on both
    for ([_]u16{ leader_port, follower_port }) |port| {
        const stream = try connectClient(port);
        defer stream.close();
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 20300, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    // Random write at offset 8192 via leader, replicating to follower
    var addr_buf: [64]u8 = undefined;
    const follower_addr = std.fmt.bufPrint(&addr_buf, "/127.0.0.1:{d}", .{follower_port}) catch unreachable;

    const data = "random write replication test";
    const crc = crc32_mod.hash(data);
    {
        const stream = try connectClient(leader_port);
        defer stream.close();
        const req = Packet{
            .opcode = proto.OP_RANDOM_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20300,
            .extent_offset = 8192,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .remaining_followers = 1,
            .arg_len = @intCast(follower_addr.len),
            .arg = follower_addr,
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    std.time.sleep(50 * std.time.ns_per_ms);

    // Read from follower at the same offset
    {
        const stream = try connectClient(follower_port);
        defer stream.close();
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 20300,
            .extent_offset = 8192,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
        try testing.expectEqualStrings(data, resp.data);
    }
}

test "functional: replication — multiple writes replicated in sequence" {
    const allocator = testing.allocator;
    var rh = try ReplicationTestHarness.setup(2);
    defer rh.teardown();

    const leader_port = rh.nodes[0].server_port;
    const follower_port = rh.nodes[1].server_port;

    // Create extent on both
    for ([_]u16{ leader_port, follower_port }) |port| {
        const stream = try connectClient(port);
        defer stream.close();
        const req = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = TEST_PARTITION_ID, .extent_id = 20400, .req_id = nextReqId() };
        var resp = try roundtrip(stream, &req, allocator);
        defer resp.deinit();
        try testing.expectEqual(proto.OP_OK, resp.result_code);
    }

    var addr_buf: [64]u8 = undefined;
    const follower_addr = std.fmt.bufPrint(&addr_buf, "/127.0.0.1:{d}", .{follower_port}) catch unreachable;

    // Write 3 chunks at different offsets
    const chunks = [_]struct { offset: i64, data: []const u8 }{
        .{ .offset = 0, .data = "chunk-0-first-data" },
        .{ .offset = 4096, .data = "chunk-1-second-data" },
        .{ .offset = 8192, .data = "chunk-2-third-data" },
    };

    {
        const leader_stream = try connectClient(leader_port);
        defer leader_stream.close();

        for (chunks) |chunk| {
            const crc = crc32_mod.hash(chunk.data);
            const req = Packet{
                .opcode = proto.OP_RANDOM_WRITE,
                .partition_id = TEST_PARTITION_ID,
                .extent_id = 20400,
                .extent_offset = chunk.offset,
                .size = @intCast(chunk.data.len),
                .crc = crc,
                .data = chunk.data,
                .remaining_followers = 1,
                .arg_len = @intCast(follower_addr.len),
                .arg = follower_addr,
                .req_id = nextReqId(),
            };
            var resp = try roundtrip(leader_stream, &req, allocator);
            defer resp.deinit();
            try testing.expectEqual(proto.OP_OK, resp.result_code);
        }
    }

    std.time.sleep(200 * std.time.ns_per_ms);

    // Verify all chunks on follower
    {
        const follower_stream = try connectClient(follower_port);
        defer follower_stream.close();

        for (chunks) |chunk| {
            const req = Packet{
                .opcode = proto.OP_READ,
                .result_code = proto.OP_INIT_RESULT_CODE,
                .partition_id = TEST_PARTITION_ID,
                .extent_id = 20400,
                .extent_offset = chunk.offset,
                .size = @intCast(chunk.data.len),
                .req_id = nextReqId(),
            };
            var resp = try roundtrip(follower_stream, &req, allocator);
            defer resp.deinit();
            try testing.expectEqual(proto.OP_OK, resp.result_code);
            try testing.expectEqualStrings(chunk.data, resp.data);
        }
    }
}
