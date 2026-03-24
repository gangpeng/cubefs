// e2e_live_test.zig — Live end-to-end test against a running cfs-datanode-zig.
//
// This program connects to a live datanode on 127.0.0.1:17410,
// exercises the full packet lifecycle, and tests admin endpoints.
//
// Build: zig build-exe src/e2e_live_test.zig -lc
// Run:   ./e2e_live_test

const std = @import("std");
const net = std.net;
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const codec = @import("codec.zig");
const crc32_mod = @import("crc32.zig");
const Packet = pkt_mod.Packet;

const SERVER_IP = "127.0.0.1";
const SERVER_PORT: u16 = 17410;
const ADMIN_PORT: u16 = 17411;
const TEST_PARTITION_ID: u64 = 10001;

var allocator: std.mem.Allocator = undefined;
var passed: u32 = 0;
var failed: u32 = 0;
var next_req: i64 = 1;

fn nextReqId() i64 {
    const id = next_req;
    next_req += 1;
    return id;
}

fn connect() !net.Stream {
    const address = try net.Address.parseIp4(SERVER_IP, SERVER_PORT);
    return try net.tcpConnectToAddress(address);
}

fn roundtrip(stream: net.Stream, pkt: *const Packet) !Packet {
    try codec.writePacket(stream.writer(), pkt);
    return try codec.readPacket(stream.reader(), allocator);
}

fn pass(name: []const u8) void {
    std.debug.print("  \x1b[32mPASS\x1b[0m {s}\n", .{name});
    passed += 1;
}

fn fail(name: []const u8, msg: []const u8) void {
    std.debug.print("  \x1b[31mFAIL\x1b[0m {s}: {s}\n", .{ name, msg });
    failed += 1;
}

// ── Test: Create Partition (via heartbeat to seed) ──────────────────

fn testCreateExtent(stream: net.Stream) void {
    const name = "create extent";
    const req = Packet{
        .opcode = proto.OP_CREATE_EXTENT,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = 1001,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();

    if (resp.result_code == proto.OP_OK) {
        pass(name);
    } else {
        fail(name, "unexpected result code");
    }
}

fn testCreateExtentAutoId(stream: net.Stream) void {
    const name = "create extent (auto-ID)";
    const req = Packet{
        .opcode = proto.OP_CREATE_EXTENT,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = 0,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();

    if (resp.result_code == proto.OP_OK and resp.extent_id >= 1024) {
        pass(name);
    } else {
        fail(name, "unexpected result or extent_id");
    }
}

fn testDuplicateExtent(stream: net.Stream) void {
    const name = "duplicate extent";
    const req = Packet{
        .opcode = proto.OP_CREATE_EXTENT,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = 1001,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();

    if (resp.result_code == proto.OP_EXIST_ERR) {
        pass(name);
    } else {
        fail(name, "expected OP_EXIST_ERR");
    }
}

fn testWriteAndRead(stream: net.Stream) void {
    const name = "write + read";
    // Create extent 2001
    {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2001,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "create roundtrip error");
            return;
        };
        resp.deinit();
    }

    // Write
    const data = "Hello from Zig E2E live test!";
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
        var resp = roundtrip(stream, &req) catch {
            fail(name, "write roundtrip error");
            return;
        };
        defer resp.deinit();
        if (resp.result_code != proto.OP_OK) {
            fail(name, "write failed");
            return;
        }
        if (resp.crc != crc) {
            fail(name, "write CRC mismatch");
            return;
        }
    }

    // Read
    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2001,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "read roundtrip error");
            return;
        };
        defer resp.deinit();
        if (resp.result_code != proto.OP_OK) {
            fail(name, "read failed");
            return;
        }
        if (!std.mem.eql(u8, data, resp.data)) {
            fail(name, "data mismatch");
            return;
        }
        if (resp.crc != crc) {
            fail(name, "read CRC mismatch");
            return;
        }
    }

    pass(name);
}

fn testStreamRead(stream: net.Stream) void {
    const name = "stream read";
    // Create extent 2002
    {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2002,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "create error");
            return;
        };
        resp.deinit();
    }

    const data = "stream read e2e data";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2002,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "write error");
            return;
        };
        resp.deinit();
    }

    {
        const req = Packet{
            .opcode = proto.OP_STREAM_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2002,
            .extent_offset = 0,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "stream read error");
            return;
        };
        defer resp.deinit();
        if (resp.result_code != proto.OP_OK or !std.mem.eql(u8, data, resp.data)) {
            fail(name, "stream read mismatch");
            return;
        }
    }

    pass(name);
}

fn testRandomWrite(stream: net.Stream) void {
    const name = "random write + read";
    {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2003,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "create error");
            return;
        };
        resp.deinit();
    }

    const data = "random write at offset 8192";
    const crc = crc32_mod.hash(data);
    {
        const req = Packet{
            .opcode = proto.OP_RANDOM_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2003,
            .extent_offset = 8192,
            .size = @intCast(data.len),
            .crc = crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "write error");
            return;
        };
        resp.deinit();
    }

    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2003,
            .extent_offset = 8192,
            .size = @intCast(data.len),
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "read error");
            return;
        };
        defer resp.deinit();
        if (resp.result_code != proto.OP_OK or !std.mem.eql(u8, data, resp.data)) {
            fail(name, "data mismatch");
            return;
        }
    }

    pass(name);
}

fn testLargeWrite(stream: net.Stream) void {
    const name = "large write 128KB";
    {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2004,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "create error");
            return;
        };
        resp.deinit();
    }

    const size: usize = 128 * 1024;
    const data = allocator.alloc(u8, size) catch {
        fail(name, "alloc error");
        return;
    };
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 131 +% 17);
    const crc = crc32_mod.hash(data);

    {
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2004,
            .extent_offset = 0,
            .size = @intCast(size),
            .crc = crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "write error");
            return;
        };
        resp.deinit();
    }

    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2004,
            .extent_offset = 0,
            .size = @intCast(size),
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "read error");
            return;
        };
        defer resp.deinit();
        if (resp.result_code != proto.OP_OK) {
            fail(name, "read failed");
            return;
        }
        if (!std.mem.eql(u8, data, resp.data)) {
            fail(name, "128KB data mismatch");
            return;
        }
        if (resp.crc != crc) {
            fail(name, "128KB CRC mismatch");
            return;
        }
    }

    pass(name);
}

fn testMarkDelete(stream: net.Stream) void {
    const name = "mark delete + read fails";
    {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2005,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "create error");
            return;
        };
        resp.deinit();
    }

    {
        const req = Packet{
            .opcode = proto.OP_MARK_DELETE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2005,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "delete error");
            return;
        };
        defer resp.deinit();
        if (resp.result_code != proto.OP_OK) {
            fail(name, "delete failed");
            return;
        }
    }

    {
        const req = Packet{
            .opcode = proto.OP_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 2005,
            .extent_offset = 0,
            .size = 100,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            fail(name, "read error");
            return;
        };
        defer resp.deinit();
        if (resp.result_code == proto.OP_NOT_EXIST_ERR) {
            pass(name);
        } else {
            fail(name, "expected NOT_EXIST after delete");
        }
    }
}

fn testGetWatermarks(stream: net.Stream) void {
    const name = "get watermarks";
    const req = Packet{
        .opcode = proto.OP_GET_ALL_WATERMARKS,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_OK and resp.size > 0) {
        pass(name);
    } else {
        fail(name, "unexpected result");
    }
}

fn testGetAppliedId(stream: net.Stream) void {
    const name = "get applied id";
    const req = Packet{
        .opcode = proto.OP_GET_APPLIED_ID,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_OK and resp.size == 8) {
        pass(name);
    } else {
        fail(name, "unexpected result");
    }
}

fn testGetPartitionSize(stream: net.Stream) void {
    const name = "get partition size";
    const req = Packet{
        .opcode = proto.OP_GET_PARTITION_SIZE,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_OK and resp.size == 16) {
        const used_size = std.mem.readInt(u64, resp.data[0..8], .big);
        const count = std.mem.readInt(u64, resp.data[8..16], .big);
        std.debug.print("           used_size={d} extent_count={d}\n", .{ used_size, count });
        pass(name);
    } else {
        fail(name, "unexpected result");
    }
}

fn testGetMaxExtentId(stream: net.Stream) void {
    const name = "get max extent id + partition size";
    const req = Packet{
        .opcode = proto.OP_GET_MAX_EXTENT_ID_AND_PARTITION_SIZE,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_OK and resp.size == 24) {
        const max_id = std.mem.readInt(u64, resp.data[0..8], .big);
        const used = std.mem.readInt(u64, resp.data[8..16], .big);
        const count = std.mem.readInt(u64, resp.data[16..24], .big);
        std.debug.print("           max_id={d} used={d} count={d}\n", .{ max_id, used, count });
        pass(name);
    } else {
        fail(name, "unexpected result");
    }
}

fn testBroadcastMinAppliedId(stream: net.Stream) void {
    const name = "broadcast min applied id";
    var arg_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &arg_buf, 100, .big);

    const req = Packet{
        .opcode = proto.OP_BROADCAST_MIN_APPLIED_ID,
        .partition_id = TEST_PARTITION_ID,
        .arg_len = 8,
        .arg = &arg_buf,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();

    if (resp.result_code != proto.OP_OK) {
        fail(name, "broadcast failed");
        return;
    }

    // Verify
    const get_req = Packet{
        .opcode = proto.OP_GET_APPLIED_ID,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var get_resp = roundtrip(stream, &get_req) catch {
        fail(name, "verify error");
        return;
    };
    defer get_resp.deinit();
    const applied = std.mem.readInt(u64, get_resp.data[0..8], .big);
    if (applied == 100) {
        pass(name);
    } else {
        fail(name, "applied id mismatch");
    }
}

fn testHeartbeat(stream: net.Stream) void {
    const name = "heartbeat";
    const req = Packet{
        .opcode = proto.OP_DATA_NODE_HEARTBEAT,
        .partition_id = 0,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_OK and resp.size > 0) {
        std.debug.print("           response_size={d} bytes\n", .{resp.size});
        pass(name);
    } else {
        fail(name, "unexpected result");
    }
}

fn testUnknownOpcode(stream: net.Stream) void {
    const name = "unknown opcode returns error";
    const req = Packet{
        .opcode = 0xEE,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_ERR) {
        pass(name);
    } else {
        fail(name, "expected OP_ERR");
    }
}

fn testPartitionNotFound(stream: net.Stream) void {
    const name = "partition not found";
    const req = Packet{
        .opcode = proto.OP_READ,
        .result_code = proto.OP_INIT_RESULT_CODE,
        .partition_id = 99999,
        .extent_id = 1,
        .size = 100,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_NOT_EXIST_ERR) {
        pass(name);
    } else {
        fail(name, "expected NOT_EXIST");
    }
}

fn testMultipleOps(stream: net.Stream) void {
    const name = "multiple ops on single connection";
    // All the previous tests ran on the same connection,
    // so this is effectively already tested.
    // We just verify we can still do a simple roundtrip.
    const req = Packet{
        .opcode = proto.OP_GET_APPLIED_ID,
        .partition_id = TEST_PARTITION_ID,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "roundtrip error");
        return;
    };
    defer resp.deinit();
    if (resp.result_code == proto.OP_OK) {
        pass(name);
    } else {
        fail(name, "unexpected result");
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    allocator = gpa.allocator();

    std.debug.print("\n\x1b[1m══════════════════════════════════════════════════════\x1b[0m\n", .{});
    std.debug.print("\x1b[1m  CubeFS Zig DataNode — Live E2E Test\x1b[0m\n", .{});
    std.debug.print("\x1b[1m  Target: {s}:{d}\x1b[0m\n", .{ SERVER_IP, SERVER_PORT });
    std.debug.print("\x1b[1m══════════════════════════════════════════════════════\x1b[0m\n\n", .{});

    // Connect
    std.debug.print("\x1b[1m─── Connecting... ───\x1b[0m\n", .{});
    const stream = connect() catch {
        std.debug.print("\x1b[31mFATAL: Cannot connect to {s}:{d}\x1b[0m\n", .{ SERVER_IP, SERVER_PORT });
        std.debug.print("Make sure cfs-datanode-zig is running with the test config.\n", .{});
        std.process.exit(1);
    };
    defer stream.close();
    std.debug.print("  Connected.\n\n", .{});

    // We need a partition first. Create one via the protocol.
    // The datanode starts with disks but no partitions, so we create one.
    // First, check if partition exists via a read (will get NOT_EXIST for partition).
    // Then create the partition directory and use handler_master to create it.
    // Actually, the CREATE_EXTENT will fail with NOT_EXIST for the partition.
    // We need to create the partition manually before testing.

    std.debug.print("\x1b[1m─── Setting up test partition {d}... ───\x1b[0m\n", .{TEST_PARTITION_ID});
    // Create the partition directory manually
    std.fs.cwd().makePath("/tmp/cubefs_e2e_test/disk0/datapartition_10001") catch {};

    // Try creating an extent — if partition doesn't exist yet, we need to handle that
    {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = TEST_PARTITION_ID,
            .extent_id = 999,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &req) catch {
            std.debug.print("  \x1b[33mPartition not loaded. Testing with available partitions.\x1b[0m\n", .{});
            std.debug.print("  Note: The server needs to be restarted after creating the partition directory.\n", .{});
            std.process.exit(1);
        };
        defer resp.deinit();

        if (resp.result_code == proto.OP_NOT_EXIST_ERR) {
            std.debug.print("  \x1b[33mPartition {d} not found. Server needs to scan disks.\x1b[0m\n", .{TEST_PARTITION_ID});
            std.debug.print("  The partition directory was created. Restart the server to load it.\n", .{});
            std.process.exit(1);
        }
        std.debug.print("  Partition ready (probe returned code={d}).\n\n", .{resp.result_code});
    }

    // ── Protocol Tests ──
    std.debug.print("\x1b[1m─── Protocol Tests ───\x1b[0m\n", .{});

    testCreateExtent(stream);
    testCreateExtentAutoId(stream);
    testDuplicateExtent(stream);
    testWriteAndRead(stream);
    testStreamRead(stream);
    testRandomWrite(stream);
    testLargeWrite(stream);
    testMarkDelete(stream);
    testGetWatermarks(stream);
    testGetAppliedId(stream);
    testGetPartitionSize(stream);
    testGetMaxExtentId(stream);
    testBroadcastMinAppliedId(stream);
    testHeartbeat(stream);
    testUnknownOpcode(stream);
    testPartitionNotFound(stream);
    testMultipleOps(stream);

    // ── Results ──
    std.debug.print("\n\x1b[1m══════════════════════════════════════════════════════\x1b[0m\n", .{});
    const total = passed + failed;
    if (failed == 0) {
        std.debug.print("\x1b[32m  All {d} tests passed!\x1b[0m\n", .{total});
    } else {
        std.debug.print("\x1b[31m  {d}/{d} tests failed\x1b[0m\n", .{ failed, total });
    }
    std.debug.print("\x1b[1m══════════════════════════════════════════════════════\x1b[0m\n\n", .{});

    if (failed > 0) {
        std.process.exit(1);
    }
}
