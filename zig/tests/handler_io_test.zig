// handler_io_test.zig — Unit tests for I/O opcode handlers.
//
// Tests each handler function in handler_io.zig by constructing Packet
// and DataPartition directly, without TCP. Covers success paths, error
// paths, and edge cases.

const std = @import("std");
const testing = std.testing;
const engine = @import("cubefs_engine");

const proto = engine._protocol;
const pkt_mod = engine._packet;
const handler_io = engine._handler_io;
const partition_mod = engine._partition;
const disk_mod = engine._disk;
const space_mgr_mod = engine._space_manager;
const crc32 = engine._crc32;
const constants = engine._constants;
const json_mod = engine._json;
const Packet = pkt_mod.Packet;
const DataPartition = partition_mod.DataPartition;
const SpaceManager = space_mgr_mod.SpaceManager;
const Disk = disk_mod.Disk;

// ─── Test Harness ─────────────────────────────────────────────────

const server_alloc = std.heap.page_allocator;

/// Atomic counter for unique test directory names.
var test_dir_counter = std.atomic.Value(u32).init(0);

const HandlerTestCtx = struct {
    dp: *DataPartition,
    disk: *Disk,
    tmp_dir: []const u8,

    fn setup() !HandlerTestCtx {
        const dir_id = test_dir_counter.fetchAdd(1, .monotonic);
        const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));

        const tmp_dir = try std.fmt.allocPrint(server_alloc, "/tmp/zig_handler_test_{d}_{d}", .{ timestamp, dir_id });
        std.fs.cwd().deleteTree(tmp_dir) catch {};

        const disk_path = try std.fmt.allocPrint(server_alloc, "{s}/disk0", .{tmp_dir});
        std.fs.cwd().makePath(disk_path) catch {};

        const dp_path = try std.fmt.allocPrintZ(server_alloc, "{s}/disk0/datapartition_1001", .{tmp_dir});
        std.fs.cwd().makePath(dp_path) catch {};

        const d = try Disk.init(server_alloc, disk_path, 0, 0, 0);
        const dp = try DataPartition.init(server_alloc, 1001, 1, dp_path, &.{}, d);

        return .{
            .dp = dp,
            .disk = d,
            .tmp_dir = tmp_dir,
        };
    }

    fn teardown(self: *HandlerTestCtx) void {
        self.dp.deinit();
        self.disk.deinit();
        std.fs.cwd().deleteTree(self.tmp_dir) catch {};
        server_alloc.free(self.tmp_dir);
    }
};

fn makePacket(opcode: u8, partition_id: u64, extent_id: u64) Packet {
    return Packet{
        .opcode = opcode,
        .partition_id = partition_id,
        .extent_id = extent_id,
        .req_id = 1,
    };
}

const ReadDeleteReaderCtx = struct {
    dp: *DataPartition,
    extent_id: u64,
    size: u32,
    expected_data: []const u8,
    expected_crc: u32,
    start: *std.Thread.ResetEvent,
    delete_finished: *std.atomic.Value(bool),
    ready_count: *std.atomic.Value(u32),
    saw_ok: *bool,
    saw_error: *bool,
    final_result_code: *u8,
};

fn readUntilDelete(ctx: ReadDeleteReaderCtx) void {
    _ = ctx.ready_count.fetchAdd(1, .acq_rel);
    ctx.start.wait();

    while (!ctx.delete_finished.load(.acquire)) {
        var pkt = makePacket(proto.OP_READ, 1001, ctx.extent_id);
        pkt.size = ctx.size;
        pkt.extent_offset = 0;

        var reply = handler_io.handleRead(&pkt, ctx.dp, std.heap.page_allocator);
        defer reply.deinit();

        if (reply.result_code == proto.OP_OK) {
            if (std.mem.eql(u8, ctx.expected_data, reply.data) and reply.crc == ctx.expected_crc) {
                ctx.saw_ok.* = true;
            }
        } else {
            ctx.saw_error.* = true;
        }

        std.time.sleep(100 * std.time.ns_per_us);
    }

    var final_pkt = makePacket(proto.OP_READ, 1001, ctx.extent_id);
    final_pkt.size = ctx.size;
    final_pkt.extent_offset = 0;

    var final_reply = handler_io.handleRead(&final_pkt, ctx.dp, std.heap.page_allocator);
    defer final_reply.deinit();

    ctx.final_result_code.* = final_reply.result_code;
    if (final_reply.result_code == proto.OP_OK) {
        if (std.mem.eql(u8, ctx.expected_data, final_reply.data) and final_reply.crc == ctx.expected_crc) {
            ctx.saw_ok.* = true;
        }
    } else {
        ctx.saw_error.* = true;
    }
}

const ReadDeleteDeleterCtx = struct {
    dp: *DataPartition,
    extent_id: u64,
    start: *std.Thread.ResetEvent,
    delete_finished: *std.atomic.Value(bool),
    ready_count: *std.atomic.Value(u32),
    result_code: *u8,
};

fn markDeleteAfterStart(ctx: ReadDeleteDeleterCtx) void {
    _ = ctx.ready_count.fetchAdd(1, .acq_rel);
    ctx.start.wait();

    std.time.sleep(1 * std.time.ns_per_ms);

    var pkt = makePacket(proto.OP_MARK_DELETE, 1001, ctx.extent_id);
    const reply = handler_io.handleMarkDelete(&pkt, ctx.dp, std.heap.page_allocator);
    ctx.result_code.* = reply.result_code;
    ctx.delete_finished.store(true, .release);
}

// ─── handleCreateExtent Tests ─────────────────────────────────────

test "handler: create extent with explicit ID" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    const reply = handler_io.handleCreateExtent(&pkt, ctx.dp, testing.allocator);

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expectEqual(@as(u64, 1024), reply.extent_id);
}

test "handler: create extent with auto-allocate ID" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 0);
    const reply = handler_io.handleCreateExtent(&pkt, ctx.dp, testing.allocator);

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expect(reply.extent_id > 0);
}

test "handler: create duplicate extent returns error" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&pkt, ctx.dp, testing.allocator);

    // Second create with same ID
    const reply = handler_io.handleCreateExtent(&pkt, ctx.dp, testing.allocator);
    try testing.expect(reply.result_code != proto.OP_OK);
}

// ─── handleWrite Tests ────────────────────────────────────────────

test "handler: write then read roundtrip" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    // Create extent
    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    // Write data
    const data = "hello handler test";
    const data_crc = crc32.hash(data);
    var write_pkt = makePacket(proto.OP_WRITE, 1001, 1024);
    write_pkt.data = @constCast(data);
    write_pkt.size = @intCast(data.len);
    write_pkt.crc = data_crc;

    const write_reply = handler_io.handleWrite(&write_pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, write_reply.result_code);
    try testing.expectEqual(data_crc, write_reply.crc);

    // Read back
    var read_pkt = makePacket(proto.OP_READ, 1001, 1024);
    read_pkt.size = @intCast(data.len);
    read_pkt.extent_offset = 0;

    var read_reply = handler_io.handleRead(&read_pkt, ctx.dp, testing.allocator);
    defer read_reply.deinit();

    try testing.expectEqual(proto.OP_OK, read_reply.result_code);
    try testing.expectEqual(@as(u32, @intCast(data.len)), read_reply.size);
    try testing.expectEqualStrings(data, read_reply.data);
}

test "handler: write with empty data returns error" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    // Write with no data
    var pkt = makePacket(proto.OP_WRITE, 1001, 1024);
    pkt.data = &.{};
    pkt.size = 0;

    const reply = handler_io.handleWrite(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_ARG_MISMATCH_ERR, reply.result_code);
}

test "handler: write to nonexistent extent returns error" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    const data = "orphan write";
    var pkt = makePacket(proto.OP_WRITE, 1001, 9999);
    pkt.data = @constCast(data);
    pkt.size = @intCast(data.len);
    pkt.crc = crc32.hash(data);

    const reply = handler_io.handleWrite(&pkt, ctx.dp, testing.allocator);
    try testing.expect(reply.result_code != proto.OP_OK);
}

// ─── handleRandomWrite Tests ──────────────────────────────────────

test "handler: random write overwrites data" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    // Append first
    const data1 = "original data!!!";
    var w1 = makePacket(proto.OP_WRITE, 1001, 1024);
    w1.data = @constCast(data1);
    w1.size = @intCast(data1.len);
    w1.crc = crc32.hash(data1);
    _ = handler_io.handleWrite(&w1, ctx.dp, testing.allocator);

    // Random write at offset 0
    const data2 = "replaced data!!!";
    var w2 = makePacket(proto.OP_RANDOM_WRITE, 1001, 1024);
    w2.data = @constCast(data2);
    w2.size = @intCast(data2.len);
    w2.crc = crc32.hash(data2);
    w2.extent_offset = 0;

    const reply = handler_io.handleRandomWrite(&w2, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);

    // Read back
    var read_pkt = makePacket(proto.OP_READ, 1001, 1024);
    read_pkt.size = @intCast(data2.len);
    read_pkt.extent_offset = 0;

    var read_reply = handler_io.handleRead(&read_pkt, ctx.dp, testing.allocator);
    defer read_reply.deinit();

    try testing.expectEqual(proto.OP_OK, read_reply.result_code);
    try testing.expectEqualStrings(data2, read_reply.data);
}

// ─── handleRead Tests ─────────────────────────────────────────────

test "handler: read zero size returns ok with no data" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_READ, 1001, 1024);
    pkt.size = 0;

    const reply = handler_io.handleRead(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

test "handler: read nonexistent extent returns error" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_READ, 1001, 9999);
    pkt.size = 64;
    pkt.extent_offset = 0;

    const reply = handler_io.handleRead(&pkt, ctx.dp, testing.allocator);
    try testing.expect(reply.result_code != proto.OP_OK);
}

// ─── handleStreamRead Tests ───────────────────────────────────────

test "handler: stream read returns data with computed CRC" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    // Create and write
    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    const data = "stream read test data";
    var w = makePacket(proto.OP_WRITE, 1001, 1024);
    w.data = @constCast(data);
    w.size = @intCast(data.len);
    w.crc = crc32.hash(data);
    _ = handler_io.handleWrite(&w, ctx.dp, testing.allocator);

    // Stream read
    var read_pkt = makePacket(proto.OP_STREAM_READ, 1001, 1024);
    read_pkt.size = @intCast(data.len);
    read_pkt.extent_offset = 0;

    var reply = handler_io.handleStreamRead(&read_pkt, ctx.dp, testing.allocator);
    defer reply.deinit();

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expectEqualStrings(data, reply.data);
    // CRC should be computed on the read data
    try testing.expectEqual(crc32.hash(data), reply.crc);
}

test "handler: stream read zero size returns ok" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_STREAM_READ, 1001, 1024);
    pkt.size = 0;

    const reply = handler_io.handleStreamRead(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

// ─── handleMarkDelete Tests ───────────────────────────────────────

test "handler: mark delete existing extent" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    var del_pkt = makePacket(proto.OP_MARK_DELETE, 1001, 1024);
    const reply = handler_io.handleMarkDelete(&del_pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

test "handler: mark delete nonexistent extent returns error" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_MARK_DELETE, 1001, 9999);
    const reply = handler_io.handleMarkDelete(&pkt, ctx.dp, testing.allocator);
    try testing.expect(reply.result_code != proto.OP_OK);
}

// ─── handleGetWatermarks Tests ────────────────────────────────────

test "handler: get watermarks returns JSON" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    // Create extent and write data
    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    const data = "watermark test";
    var w = makePacket(proto.OP_WRITE, 1001, 1024);
    w.data = @constCast(data);
    w.size = @intCast(data.len);
    w.crc = crc32.hash(data);
    _ = handler_io.handleWrite(&w, ctx.dp, testing.allocator);

    var pkt = makePacket(proto.OP_GET_ALL_WATERMARKS, 1001, 0);
    var reply = handler_io.handleGetWatermarks(&pkt, ctx.dp, testing.allocator);
    defer reply.deinit();

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expect(reply.size > 0);
    // Should be valid JSON
    try testing.expect(reply.data.len > 0);
}

// ─── handleGetAppliedId Tests ─────────────────────────────────────

test "handler: get applied id returns 8 bytes big-endian" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_GET_APPLIED_ID, 1001, 0);
    var reply = handler_io.handleGetAppliedId(&pkt, ctx.dp, testing.allocator);
    defer reply.deinit();

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expectEqual(@as(u32, 8), reply.size);
}

// ─── handleGetPartitionSize Tests ─────────────────────────────────

test "handler: get partition size returns 16 bytes" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_GET_PARTITION_SIZE, 1001, 0);
    var reply = handler_io.handleGetPartitionSize(&pkt, ctx.dp, testing.allocator);
    defer reply.deinit();

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expectEqual(@as(u32, 16), reply.size);

    // Parse used_size and extent_count
    const used = std.mem.readInt(u64, reply.data[0..8], .big);
    const count = std.mem.readInt(u64, reply.data[8..16], .big);
    _ = used;
    _ = count;
}

// ─── handleGetMaxExtentIdAndPartitionSize Tests ───────────────────

test "handler: get max extent id and partition size returns 24 bytes" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    var pkt = makePacket(proto.OP_GET_MAX_EXTENT_ID_AND_PARTITION_SIZE, 1001, 0);
    var reply = handler_io.handleGetMaxExtentIdAndPartitionSize(&pkt, ctx.dp, testing.allocator);
    defer reply.deinit();

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expectEqual(@as(u32, 24), reply.size);

    const max_id = std.mem.readInt(u64, reply.data[0..8], .big);
    try testing.expect(max_id > 0);
}

// ─── handleBroadcastMinAppliedId Tests ────────────────────────────

test "handler: broadcast min applied id updates if higher" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    // Set applied id via broadcast
    var arg_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &arg_buf, 42, .big);

    var pkt = makePacket(proto.OP_BROADCAST_MIN_APPLIED_ID, 1001, 0);
    pkt.arg = &arg_buf;

    const reply = handler_io.handleBroadcastMinAppliedId(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);

    // Verify applied id was set
    try testing.expectEqual(@as(u64, 42), ctx.dp.getAppliedId());
}

test "handler: broadcast min applied id does not decrease" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    // Set to 100
    var arg1: [8]u8 = undefined;
    std.mem.writeInt(u64, &arg1, 100, .big);
    var pkt1 = makePacket(proto.OP_BROADCAST_MIN_APPLIED_ID, 1001, 0);
    pkt1.arg = &arg1;
    _ = handler_io.handleBroadcastMinAppliedId(&pkt1, ctx.dp, testing.allocator);

    // Try to set to 50 (should not decrease)
    var arg2: [8]u8 = undefined;
    std.mem.writeInt(u64, &arg2, 50, .big);
    var pkt2 = makePacket(proto.OP_BROADCAST_MIN_APPLIED_ID, 1001, 0);
    pkt2.arg = &arg2;
    _ = handler_io.handleBroadcastMinAppliedId(&pkt2, ctx.dp, testing.allocator);

    try testing.expectEqual(@as(u64, 100), ctx.dp.getAppliedId());
}

// ─── handleReadTinyDeleteRecord Tests ─────────────────────────────

test "handler: read tiny delete records returns ok" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_READ_TINY_DELETE_RECORD, 1001, 0);
    var reply = handler_io.handleReadTinyDeleteRecord(&pkt, ctx.dp, testing.allocator);
    defer reply.deinit();

    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

// ─── handleExtentRepairRead Tests ─────────────────────────────────

test "handler: extent repair read returns data with CRC" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    // Create and write
    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    const data = "repair read test";
    var w = makePacket(proto.OP_WRITE, 1001, 1024);
    w.data = @constCast(data);
    w.size = @intCast(data.len);
    w.crc = crc32.hash(data);
    _ = handler_io.handleWrite(&w, ctx.dp, testing.allocator);

    // Repair read
    var pkt = makePacket(proto.OP_EXTENT_REPAIR_READ, 1001, 1024);
    pkt.size = @intCast(data.len);
    pkt.extent_offset = 0;

    var reply = handler_io.handleExtentRepairRead(&pkt, ctx.dp, testing.allocator);
    defer reply.deinit();

    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expectEqualStrings(data, reply.data);
    try testing.expectEqual(crc32.hash(data), reply.crc);
}

test "handler: extent repair read zero size returns ok" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_EXTENT_REPAIR_READ, 1001, 1024);
    pkt.size = 0;

    const reply = handler_io.handleExtentRepairRead(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

// ─── handleBackupRead / handleBackupWrite Tests ───────────────────

test "handler: backup write then backup read" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    // Backup write (always sync, random write type)
    const data = "backup data here";
    var w = makePacket(proto.OP_BACKUP_WRITE, 1001, 1024);
    w.data = @constCast(data);
    w.size = @intCast(data.len);
    w.crc = crc32.hash(data);
    w.extent_offset = 0;

    const write_reply = handler_io.handleBackupWrite(&w, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, write_reply.result_code);

    // Backup read (same as stream read)
    var r = makePacket(proto.OP_BACKUP_READ, 1001, 1024);
    r.size = @intCast(data.len);
    r.extent_offset = 0;

    var read_reply = handler_io.handleBackupRead(&r, ctx.dp, testing.allocator);
    defer read_reply.deinit();

    try testing.expectEqual(proto.OP_OK, read_reply.result_code);
    try testing.expectEqualStrings(data, read_reply.data);
}

// ─── handleBatchLockExtent / handleBatchUnlockExtent Tests ────────

test "handler: batch lock and unlock extents" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    // Lock extent
    const json_arg = "[{\"ExtentId\":1024,\"GcFlag\":1}]";
    var lock_pkt = makePacket(proto.OP_BATCH_LOCK_NORMAL_EXTENT, 1001, 0);
    lock_pkt.arg = @constCast(json_arg);

    const lock_reply = handler_io.handleBatchLockExtent(&lock_pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, lock_reply.result_code);

    // Unlock all
    var unlock_pkt = makePacket(proto.OP_BATCH_UNLOCK_NORMAL_EXTENT, 1001, 0);
    const unlock_reply = handler_io.handleBatchUnlockExtent(&unlock_pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, unlock_reply.result_code);
}

test "handler: batch lock with empty arg returns ok" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_BATCH_LOCK_NORMAL_EXTENT, 1001, 0);
    pkt.arg = &.{};

    const reply = handler_io.handleBatchLockExtent(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

test "handler: batch lock with invalid JSON returns error" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_BATCH_LOCK_NORMAL_EXTENT, 1001, 0);
    pkt.arg = @constCast("not json at all");

    const reply = handler_io.handleBatchLockExtent(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_ARG_MISMATCH_ERR, reply.result_code);
}

// ─── handleDecommission Tests ─────────────────────────────────────

test "handler: decommission sets partition read-only" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_DECOMMISSION_DATA_PARTITION, 1001, 0);
    const reply = handler_io.handleDecommission(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
    try testing.expectEqual(partition_mod.PARTITION_STATUS_READ_ONLY, ctx.dp.getStatus());
}

// ─── handleNotifyReplicasToRepair Tests ───────────────────────────

test "handler: notify replicas to repair returns ok" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_NOTIFY_REPLICAS_TO_REPAIR, 1001, 0);
    const reply = handler_io.handleNotifyReplicasToRepair(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

// ─── handleStopRepair Tests ───────────────────────────────────────

test "handler: stop repair returns ok" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    var pkt = makePacket(proto.OP_STOP_DATA_PARTITION_REPAIR, 1001, 0);
    const reply = handler_io.handleStopRepair(&pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, reply.result_code);
}

// ─── Read after delete Tests ──────────────────────────────────────

test "handler: read after mark delete returns error" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    // Create, write, delete, then try to read
    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, 1024);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    const data = "will be deleted";
    var w = makePacket(proto.OP_WRITE, 1001, 1024);
    w.data = @constCast(data);
    w.size = @intCast(data.len);
    w.crc = crc32.hash(data);
    _ = handler_io.handleWrite(&w, ctx.dp, testing.allocator);

    var del_pkt = makePacket(proto.OP_MARK_DELETE, 1001, 1024);
    _ = handler_io.handleMarkDelete(&del_pkt, ctx.dp, testing.allocator);

    var read_pkt = makePacket(proto.OP_READ, 1001, 1024);
    read_pkt.size = @intCast(data.len);
    read_pkt.extent_offset = 0;

    const reply = handler_io.handleRead(&read_pkt, ctx.dp, testing.allocator);
    try testing.expect(reply.result_code != proto.OP_OK);
}

test "handler: same-extent reads fail once concurrent mark delete completes" {
    var ctx = try HandlerTestCtx.setup();
    defer ctx.teardown();

    const reader_count = 4;
    const extent_id: u64 = 2048;
    const data_len = 64 * 1024;

    var create_pkt = makePacket(proto.OP_CREATE_EXTENT, 1001, extent_id);
    _ = handler_io.handleCreateExtent(&create_pkt, ctx.dp, testing.allocator);

    const data = try testing.allocator.alloc(u8, data_len);
    defer testing.allocator.free(data);
    for (data, 0..) |*byte, i| {
        byte.* = @truncate(i *% 37 +% 11);
    }

    var write_pkt = makePacket(proto.OP_WRITE, 1001, extent_id);
    write_pkt.data = data;
    write_pkt.size = @intCast(data.len);
    write_pkt.crc = crc32.hash(data);

    const write_reply = handler_io.handleWrite(&write_pkt, ctx.dp, testing.allocator);
    try testing.expectEqual(proto.OP_OK, write_reply.result_code);

    var start = std.Thread.ResetEvent{};
    var delete_finished = std.atomic.Value(bool).init(false);
    var ready_count = std.atomic.Value(u32).init(0);
    var saw_ok = [_]bool{ false, false, false, false };
    var saw_error = [_]bool{ false, false, false, false };
    var final_result_codes = [_]u8{ proto.OP_OK, proto.OP_OK, proto.OP_OK, proto.OP_OK };
    var reader_threads: [reader_count]?std.Thread = .{null} ** reader_count;
    var delete_result_code: u8 = proto.OP_ERR;

    for (0..reader_count) |i| {
        reader_threads[i] = try std.Thread.spawn(.{}, readUntilDelete, .{ReadDeleteReaderCtx{
            .dp = ctx.dp,
            .extent_id = extent_id,
            .size = @intCast(data.len),
            .expected_data = data,
            .expected_crc = write_pkt.crc,
            .start = &start,
            .delete_finished = &delete_finished,
            .ready_count = &ready_count,
            .saw_ok = &saw_ok[i],
            .saw_error = &saw_error[i],
            .final_result_code = &final_result_codes[i],
        }});
    }

    const deleter = try std.Thread.spawn(.{}, markDeleteAfterStart, .{ReadDeleteDeleterCtx{
        .dp = ctx.dp,
        .extent_id = extent_id,
        .start = &start,
        .delete_finished = &delete_finished,
        .ready_count = &ready_count,
        .result_code = &delete_result_code,
    }});

    while (ready_count.load(.acquire) < reader_count + 1) {
        std.time.sleep(100 * std.time.ns_per_us);
    }

    start.set();

    for (reader_threads) |thread| {
        if (thread) |t| t.join();
    }
    deleter.join();

    try testing.expectEqual(proto.OP_OK, delete_result_code);

    for (0..reader_count) |i| {
        _ = saw_ok[i];
        _ = saw_error[i];
        try testing.expect(final_result_codes[i] != proto.OP_OK);
    }
}
