const std = @import("std");
const net = std.net;
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const codec = @import("codec.zig");
const crc32_mod = @import("crc32.zig");

const Packet = pkt_mod.Packet;

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_LEADER_PORT: u16 = 17410;
const DEFAULT_FOLLOWERS = [_]u16{ 17420, 17430 };
const TEST_PARTITION_ID: u64 = 10001;

var allocator: std.mem.Allocator = undefined;
var passed: u32 = 0;
var failed: u32 = 0;
var next_req: i64 = 1;

const Options = struct {
    host: []const u8,
    leader_port: u16,
    followers: []u16,
};

fn nextReqId() i64 {
    const id = next_req;
    next_req += 1;
    return id;
}

fn pass(name: []const u8) void {
    std.debug.print("  PASS {s}\n", .{name});
    passed += 1;
}

fn fail(name: []const u8, msg: []const u8) void {
    std.debug.print("  FAIL {s}: {s}\n", .{ name, msg });
    failed += 1;
}

fn connect(host: []const u8, port: u16) !net.Stream {
    const address = try net.Address.parseIp4(host, port);
    return try net.tcpConnectToAddress(address);
}

fn roundtrip(stream: net.Stream, pkt: *const Packet) !Packet {
    try codec.writePacket(stream.writer(), pkt);
    return try codec.readPacket(stream.reader(), allocator);
}

fn createExtent(host: []const u8, port: u16, extent_id: u64) !void {
    const stream = try connect(host, port);
    defer stream.close();

    const req = Packet{
        .opcode = proto.OP_CREATE_EXTENT,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = extent_id,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req);
    defer resp.deinit();

    if (resp.result_code != proto.OP_OK and resp.result_code != proto.OP_EXIST_ERR) {
        return error.UnexpectedResponse;
    }
}

fn readExact(host: []const u8, port: u16, extent_id: u64, offset: i64, size: usize) !Packet {
    const stream = try connect(host, port);
    defer stream.close();

    const req = Packet{
        .opcode = proto.OP_READ,
        .result_code = proto.OP_INIT_RESULT_CODE,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = extent_id,
        .extent_offset = offset,
        .size = @intCast(size),
        .req_id = nextReqId(),
    };
    return try roundtrip(stream, &req);
}

fn followerArg(host: []const u8, ports: []const u16, buf: []u8) ![]const u8 {
    var fbs = std.io.fixedBufferStream(buf);
    const writer = fbs.writer();
    for (ports) |port| {
        try writer.print("/{s}:{d}", .{ host, port });
    }
    return fbs.getWritten();
}

fn writeReplicated(host: []const u8, leader_port: u16, extent_id: u64, offset: i64, data: []const u8, opcode: u8, followers: []const u16, arg_buf: []u8) !Packet {
    const stream = try connect(host, leader_port);
    defer stream.close();

    const crc = crc32_mod.hash(data);
    const arg = try followerArg(host, followers, arg_buf);

    const req = Packet{
        .opcode = opcode,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = extent_id,
        .extent_offset = offset,
        .size = @intCast(data.len),
        .crc = crc,
        .data = data,
        .remaining_followers = @intCast(followers.len),
        .arg_len = @intCast(arg.len),
        .arg = arg,
        .req_id = nextReqId(),
    };
    return try roundtrip(stream, &req);
}

fn testLeaderToFollower(opts: Options) void {
    const name = "leader forwards write to follower";
    if (opts.followers.len < 1) {
        fail(name, "need at least one follower");
        return;
    }

    const extent_id: u64 = 30001;
    createExtent(opts.host, opts.leader_port, extent_id) catch {
        fail(name, "leader extent create failed");
        return;
    };
    createExtent(opts.host, opts.followers[0], extent_id) catch {
        fail(name, "follower extent create failed");
        return;
    };

    const data = "live replicated write to follower";
    var arg_buf: [128]u8 = undefined;
    var resp = writeReplicated(opts.host, opts.leader_port, extent_id, 0, data, proto.OP_WRITE, opts.followers[0..1], &arg_buf) catch {
        fail(name, "leader write failed");
        return;
    };
    defer resp.deinit();
    if (resp.result_code != proto.OP_OK) {
        fail(name, "leader write returned non-OK");
        return;
    }

    std.time.sleep(100 * std.time.ns_per_ms);

    var read_resp = readExact(opts.host, opts.followers[0], extent_id, 0, data.len) catch {
        fail(name, "follower read failed");
        return;
    };
    defer read_resp.deinit();
    if (read_resp.result_code != proto.OP_OK or !std.mem.eql(u8, read_resp.data, data)) {
        fail(name, "follower data mismatch");
        return;
    }

    pass(name);
}

fn testChainReplication(opts: Options) void {
    const name = "chain replication A->B->C";
    if (opts.followers.len < 2) {
        fail(name, "need at least two followers");
        return;
    }

    const extent_id: u64 = 30002;
    const ports = [_]u16{ opts.leader_port, opts.followers[0], opts.followers[1] };
    for (ports) |port| {
        createExtent(opts.host, port, extent_id) catch {
            fail(name, "extent create failed");
            return;
        };
    }

    const data = "live chain replication A->B->C";
    var arg_buf: [128]u8 = undefined;
    var resp = writeReplicated(opts.host, opts.leader_port, extent_id, 0, data, proto.OP_WRITE, opts.followers[0..2], &arg_buf) catch {
        fail(name, "chain write failed");
        return;
    };
    defer resp.deinit();
    if (resp.result_code != proto.OP_OK) {
        fail(name, "leader write returned non-OK");
        return;
    }

    std.time.sleep(200 * std.time.ns_per_ms);

    for (opts.followers[0..2]) |port| {
        var read_resp = readExact(opts.host, port, extent_id, 0, data.len) catch {
            fail(name, "replica read failed");
            return;
        };
        defer read_resp.deinit();
        if (read_resp.result_code != proto.OP_OK or !std.mem.eql(u8, read_resp.data, data)) {
            fail(name, "replica data mismatch");
            return;
        }
    }

    pass(name);
}

fn testUnreachableFollower(opts: Options) void {
    const name = "local write succeeds with unreachable follower";
    const extent_id: u64 = 30003;
    createExtent(opts.host, opts.leader_port, extent_id) catch {
        fail(name, "leader extent create failed");
        return;
    };

    const data = "leader survives unreachable follower";
    const stream = connect(opts.host, opts.leader_port) catch {
        fail(name, "connect leader failed");
        return;
    };
    defer stream.close();

    const bad_follower = "/127.0.0.1:1";
    const req = Packet{
        .opcode = proto.OP_WRITE,
        .partition_id = TEST_PARTITION_ID,
        .extent_id = extent_id,
        .extent_offset = 0,
        .size = @intCast(data.len),
        .crc = crc32_mod.hash(data),
        .data = data,
        .remaining_followers = 1,
        .arg_len = @intCast(bad_follower.len),
        .arg = bad_follower,
        .req_id = nextReqId(),
    };
    var resp = roundtrip(stream, &req) catch {
        fail(name, "leader write failed");
        return;
    };
    defer resp.deinit();
    if (resp.result_code != proto.OP_OK) {
        fail(name, "leader write returned non-OK");
        return;
    }

    var read_resp = readExact(opts.host, opts.leader_port, extent_id, 0, data.len) catch {
        fail(name, "leader read failed");
        return;
    };
    defer read_resp.deinit();
    if (read_resp.result_code != proto.OP_OK or !std.mem.eql(u8, read_resp.data, data)) {
        fail(name, "leader data missing after partial replication failure");
        return;
    }

    pass(name);
}

fn testSequentialRandomWrites(opts: Options) void {
    const name = "sequential random writes replicate";
    if (opts.followers.len < 1) {
        fail(name, "need at least one follower");
        return;
    }

    const extent_id: u64 = 30004;
    createExtent(opts.host, opts.leader_port, extent_id) catch {
        fail(name, "leader extent create failed");
        return;
    };
    createExtent(opts.host, opts.followers[0], extent_id) catch {
        fail(name, "follower extent create failed");
        return;
    };

    const chunks = [_]struct { offset: i64, data: []const u8 }{
        .{ .offset = 0, .data = "chunk-zero-live" },
        .{ .offset = 4096, .data = "chunk-one-live" },
        .{ .offset = 8192, .data = "chunk-two-live" },
    };
    var arg_buf: [128]u8 = undefined;

    for (chunks) |chunk| {
        var resp = writeReplicated(opts.host, opts.leader_port, extent_id, chunk.offset, chunk.data, proto.OP_RANDOM_WRITE, opts.followers[0..1], &arg_buf) catch {
            fail(name, "random write failed");
            return;
        };
        defer resp.deinit();
        if (resp.result_code != proto.OP_OK) {
            fail(name, "random write returned non-OK");
            return;
        }
    }

    std.time.sleep(200 * std.time.ns_per_ms);

    for (chunks) |chunk| {
        var read_resp = readExact(opts.host, opts.followers[0], extent_id, chunk.offset, chunk.data.len) catch {
            fail(name, "follower read failed");
            return;
        };
        defer read_resp.deinit();
        if (read_resp.result_code != proto.OP_OK or !std.mem.eql(u8, read_resp.data, chunk.data)) {
            fail(name, "replicated chunk mismatch");
            return;
        }
    }

    pass(name);
}

fn parseU16(text: []const u8) !u16 {
    return try std.fmt.parseInt(u16, text, 10);
}

fn parseFollowers(alloc: std.mem.Allocator, text: []const u8) ![]u16 {
    var list = std.ArrayList(u16).init(alloc);
    errdefer list.deinit();

    var iter = std.mem.splitScalar(u8, text, ',');
    while (iter.next()) |part| {
        if (part.len == 0) continue;
        try list.append(try parseU16(part));
    }

    return try list.toOwnedSlice();
}

fn parseOptions(alloc: std.mem.Allocator) !Options {
    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    var host = try alloc.dupe(u8, DEFAULT_HOST);
    var leader_port = DEFAULT_LEADER_PORT;
    var followers = try alloc.dupe(u16, &DEFAULT_FOLLOWERS);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--host") and i + 1 < args.len) {
            i += 1;
            alloc.free(host);
            host = try alloc.dupe(u8, args[i]);
        } else if (std.mem.eql(u8, args[i], "--leader-port") and i + 1 < args.len) {
            i += 1;
            leader_port = try parseU16(args[i]);
        } else if (std.mem.eql(u8, args[i], "--followers") and i + 1 < args.len) {
            i += 1;
            alloc.free(followers);
            followers = try parseFollowers(alloc, args[i]);
        } else if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            std.debug.print(
                "Usage: e2e-live-replication-test [--host ip] [--leader-port port] [--followers port,port]\n",
                .{},
            );
            std.process.exit(0);
        }
    }

    return .{
        .host = host,
        .leader_port = leader_port,
        .followers = followers,
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    allocator = gpa.allocator();

    const opts = try parseOptions(allocator);
    defer allocator.free(opts.host);
    defer allocator.free(opts.followers);

    std.debug.print("\nLive replication test target: {s}:{d}\n", .{ opts.host, opts.leader_port });
    std.debug.print("Followers: ", .{});
    for (opts.followers, 0..) |port, idx| {
        if (idx > 0) std.debug.print(", ", .{});
        std.debug.print("{d}", .{port});
    }
    std.debug.print("\n\n", .{});

    testLeaderToFollower(opts);
    testChainReplication(opts);
    testUnreachableFollower(opts);
    testSequentialRandomWrites(opts);

    const total = passed + failed;
    if (failed == 0) {
        std.debug.print("\nAll {d} live replication tests passed.\n", .{total});
        return;
    }

    std.debug.print("\n{d}/{d} live replication tests failed.\n", .{ failed, total });
    std.process.exit(1);
}