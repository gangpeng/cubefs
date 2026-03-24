// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Network-level benchmark for CubeFS DataNode over TCP.
//!
//! Tests the full request→response cycle (protocol encode + TCP + server processing + decode).
//! Works against both Go and Zig DataNode implementations.
//!
//! Output format matches Go benchmark output:
//!   BenchmarkDataNode_Write_4KB     N    xxx ns/op    xxx MB/s
//!
//! Usage:
//!   bench-datanode-net [--port=17410] [--host=127.0.0.1] [--partition=10001]
//!
//! The target datanode must be running with the specified partition already created.

const std = @import("std");
const net = std.net;
const engine = @import("cubefs_engine");

const proto = engine._protocol;
const pkt_mod = engine._packet;
const codec_mod = engine._codec;
const crc32 = engine._crc32;
const Packet = pkt_mod.Packet;

/// Benchmark duration target (3 seconds).
const BENCH_DURATION_NS: u64 = 3 * std.time.ns_per_s;

/// Partition ID used for benchmarks.
var PARTITION_ID: u64 = 10001;

/// Server address.
var SERVER_HOST: []const u8 = "127.0.0.1";
var SERVER_PORT: u16 = 17410;

/// Next request ID (monotonic).
var next_req_id: i64 = 1;

fn nextReqId() i64 {
    const id = next_req_id;
    next_req_id += 1;
    return id;
}

// ─── Result formatting ──────────────────────────────────────────────

fn printResult(name: []const u8, iterations: u64, elapsed_ns: u64, bytes_per_op: u64) void {
    const stdout = std.io.getStdOut().writer();
    const ns_per_op = if (iterations > 0) elapsed_ns / iterations else 0;
    const mb_per_sec = if (elapsed_ns > 0)
        @as(f64, @floatFromInt(bytes_per_op)) * @as(f64, @floatFromInt(iterations)) /
            (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0) / (1024.0 * 1024.0)
    else
        0.0;

    stdout.print("{s: <50} {d: >10}    {d: >8} ns/op    {d:.1} MB/s\n", .{
        name,
        iterations,
        ns_per_op,
        mb_per_sec,
    }) catch {};
}

fn printLatencyResult(name: []const u8, iterations: u64, elapsed_ns: u64) void {
    const stdout = std.io.getStdOut().writer();
    const ns_per_op = if (iterations > 0) elapsed_ns / iterations else 0;
    const us_per_op = @as(f64, @floatFromInt(ns_per_op)) / 1000.0;

    stdout.print("{s: <50} {d: >10}    {d: >8} ns/op    {d:.1} us/op\n", .{
        name,
        iterations,
        ns_per_op,
        us_per_op,
    }) catch {};
}

// ─── Connection helper ──────────────────────────────────────────────

fn connectToServer() !net.Stream {
    const address = try net.Address.parseIp4(SERVER_HOST, SERVER_PORT);
    return try net.tcpConnectToAddress(address);
}

fn roundtrip(stream: net.Stream, pkt: *const Packet, allocator: std.mem.Allocator) !Packet {
    try codec_mod.writePacket(stream.writer(), pkt);
    return try codec_mod.readPacket(stream.reader(), allocator);
}

// ─── Setup: create extent ───────────────────────────────────────────

fn createExtent(stream: net.Stream, extent_id: u64, allocator: std.mem.Allocator) !void {
    const req = Packet{
        .opcode = proto.OP_CREATE_EXTENT,
        .partition_id = PARTITION_ID,
        .extent_id = extent_id,
        .req_id = nextReqId(),
    };
    var resp = try roundtrip(stream, &req, allocator);
    defer resp.deinit();

    if (resp.result_code != proto.OP_OK and resp.result_code != proto.OP_EXIST_ERR) {
        return error.CreateFailed;
    }
}

// ─── Benchmark: Sequential Write ────────────────────────────────────

fn benchSeqWrite(allocator: std.mem.Allocator, size: usize) void {
    const stream = connectToServer() catch {
        std.debug.print("benchSeqWrite({d}): cannot connect to server\n", .{size});
        return;
    };
    defer stream.close();

    // Create extent for writing
    const extent_id: u64 = 2000 + @as(u64, @intCast(size));
    createExtent(stream, extent_id, allocator) catch {
        std.debug.print("benchSeqWrite({d}): cannot create extent\n", .{size});
        return;
    };

    // Prepare write data
    const data = allocator.alloc(u8, size) catch return;
    defer allocator.free(data);
    @memset(data, 0xAB);
    const data_crc = crc32.hash(data);

    var offset: i64 = 0;
    var iterations: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = PARTITION_ID,
            .extent_id = extent_id,
            .extent_offset = offset,
            .size = @intCast(size),
            .crc = data_crc,
            .data = data,
            .req_id = nextReqId(),
        };

        var resp = roundtrip(stream, &req, allocator) catch {
            std.debug.print("benchSeqWrite: roundtrip error at iteration {d}\n", .{iterations});
            return;
        };
        resp.deinit();

        if (resp.result_code != proto.OP_OK) {
            std.debug.print("benchSeqWrite: write error code {d}\n", .{resp.result_code});
            return;
        }

        offset += @intCast(size);
        iterations += 1;

        if (iterations % 64 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    const name = switch (size) {
        4 * 1024 => "BenchmarkDataNode_Write_4KB",
        64 * 1024 => "BenchmarkDataNode_Write_64KB",
        128 * 1024 => "BenchmarkDataNode_Write_128KB",
        else => "BenchmarkDataNode_Write_?",
    };
    printResult(name, iterations, elapsed, @intCast(size));
}

// ─── Benchmark: Sequential Read ─────────────────────────────────────

fn benchSeqRead(allocator: std.mem.Allocator, size: usize) void {
    const stream = connectToServer() catch {
        std.debug.print("benchSeqRead({d}): cannot connect to server\n", .{size});
        return;
    };
    defer stream.close();

    // Create and fill extent for reading (write 2MB of data)
    const extent_id: u64 = 3000 + @as(u64, @intCast(size));
    createExtent(stream, extent_id, allocator) catch return;

    const fill_size: u64 = 2 * 1024 * 1024; // 2 MB
    const chunk: usize = 64 * 1024; // 64KB chunks
    const fill_data = allocator.alloc(u8, chunk) catch return;
    defer allocator.free(fill_data);
    @memset(fill_data, 0xCD);
    const fill_crc = crc32.hash(fill_data);

    // Fill with data
    var fill_off: i64 = 0;
    while (@as(u64, @intCast(fill_off)) < fill_size) {
        const write_req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = PARTITION_ID,
            .extent_id = extent_id,
            .extent_offset = fill_off,
            .size = @intCast(chunk),
            .crc = fill_crc,
            .data = fill_data,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &write_req, allocator) catch return;
        resp.deinit();
        if (resp.result_code != proto.OP_OK) return;
        fill_off += @intCast(chunk);
    }

    // Now benchmark reads
    const max_off = fill_size - @as(u64, @intCast(size));
    var read_off: u64 = 0;
    var iterations: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        const req = Packet{
            .opcode = proto.OP_STREAM_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = PARTITION_ID,
            .extent_id = extent_id,
            .extent_offset = @intCast(read_off),
            .size = @intCast(size),
            .req_id = nextReqId(),
        };

        var resp = roundtrip(stream, &req, allocator) catch {
            std.debug.print("benchSeqRead: roundtrip error at iteration {d}\n", .{iterations});
            return;
        };
        resp.deinit();

        read_off = (read_off + size) % max_off;
        iterations += 1;

        if (iterations % 64 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    const name = switch (size) {
        4 * 1024 => "BenchmarkDataNode_Read_4KB",
        64 * 1024 => "BenchmarkDataNode_Read_64KB",
        128 * 1024 => "BenchmarkDataNode_Read_128KB",
        else => "BenchmarkDataNode_Read_?",
    };
    printResult(name, iterations, elapsed, @intCast(size));
}

// ─── Benchmark: Create Extent ───────────────────────────────────────

fn benchCreateExtent(allocator: std.mem.Allocator) void {
    const stream = connectToServer() catch {
        std.debug.print("benchCreateExtent: cannot connect to server\n", .{});
        return;
    };
    defer stream.close();

    var iterations: u64 = 0;
    var eid: u64 = 5000;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        const req = Packet{
            .opcode = proto.OP_CREATE_EXTENT,
            .partition_id = PARTITION_ID,
            .extent_id = eid,
            .req_id = nextReqId(),
        };

        var resp = roundtrip(stream, &req, allocator) catch return;
        resp.deinit();

        eid += 1;
        iterations += 1;

        if (iterations % 64 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    printLatencyResult("BenchmarkDataNode_CreateExtent", iterations, elapsed);
}

// ─── Benchmark: Concurrent Write (multi-connection) ─────────────────

const ConcWriteShared = struct {
    port: u16,
    size: usize,
    data: []const u8,
    data_crc: u32,
    stop: std.atomic.Value(bool),
    total_ops: std.atomic.Value(u64),
};

fn concWriteWorker(shared: *ConcWriteShared, worker_id: usize) void {
    const allocator = std.heap.c_allocator;

    const stream = connectToServer() catch return;
    defer stream.close();

    // Each worker gets its own extent
    const extent_id: u64 = 4000 + @as(u64, @intCast(worker_id));
    createExtent(stream, extent_id, allocator) catch return;

    var offset: i64 = 0;

    while (!shared.stop.load(.acquire)) {
        const req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = PARTITION_ID,
            .extent_id = extent_id,
            .extent_offset = offset,
            .size = @intCast(shared.size),
            .crc = shared.data_crc,
            .data = shared.data,
            .req_id = @intCast(@as(i64, @intCast(worker_id)) * 1_000_000 + @divFloor(offset, @as(i64, @intCast(shared.size)))),
        };

        var resp = roundtrip(stream, &req, allocator) catch continue;
        resp.deinit();

        if (resp.result_code == proto.OP_OK) {
            _ = shared.total_ops.fetchAdd(1, .monotonic);
        }

        offset += @intCast(shared.size);
    }
}

fn benchConcurrentWrite(allocator: std.mem.Allocator, size: usize, clients: usize) void {
    const data = allocator.alloc(u8, size) catch return;
    defer allocator.free(data);
    @memset(data, 0xAB);
    const data_crc = crc32.hash(data);

    var shared = ConcWriteShared{
        .port = SERVER_PORT,
        .size = size,
        .data = data,
        .data_crc = data_crc,
        .stop = std.atomic.Value(bool).init(false),
        .total_ops = std.atomic.Value(u64).init(0),
    };

    // Spawn worker threads
    var threads: [8]std.Thread = undefined;
    const actual_clients = @min(clients, 8);
    for (0..actual_clients) |i| {
        threads[i] = std.Thread.spawn(.{}, concWriteWorker, .{ &shared, i }) catch return;
    }

    // Let them run for BENCH_DURATION_NS
    var timer = std.time.Timer.start() catch unreachable;
    while (timer.read() < BENCH_DURATION_NS) {
        std.time.sleep(1_000_000); // 1ms poll
    }
    const elapsed = timer.read();

    // Signal stop and join
    shared.stop.store(true, .release);
    for (0..actual_clients) |i| {
        threads[i].join();
    }

    const total_ops = shared.total_ops.load(.acquire);
    const name = switch (size) {
        4 * 1024 => "BenchmarkDataNode_ConcWrite4x_4KB",
        64 * 1024 => "BenchmarkDataNode_ConcWrite4x_64KB",
        128 * 1024 => "BenchmarkDataNode_ConcWrite4x_128KB",
        else => "BenchmarkDataNode_ConcWrite4x_?",
    };
    printResult(name, total_ops, elapsed, @intCast(size));
}

// ─── Benchmark: Mixed Write+Read ────────────────────────────────────

fn benchMixed(allocator: std.mem.Allocator, size: usize) void {
    const stream = connectToServer() catch {
        std.debug.print("benchMixed: cannot connect to server\n", .{});
        return;
    };
    defer stream.close();

    // Create write extent
    const write_eid: u64 = 6000;
    createExtent(stream, write_eid, allocator) catch return;

    // Create and fill read extent
    const read_eid: u64 = 6001;
    createExtent(stream, read_eid, allocator) catch return;

    const fill_size: u64 = 2 * 1024 * 1024;
    const chunk: usize = 64 * 1024;
    const fill_data = allocator.alloc(u8, chunk) catch return;
    defer allocator.free(fill_data);
    @memset(fill_data, 0xCD);
    const fill_crc = crc32.hash(fill_data);

    var fill_off: i64 = 0;
    while (@as(u64, @intCast(fill_off)) < fill_size) {
        const write_req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = PARTITION_ID,
            .extent_id = read_eid,
            .extent_offset = fill_off,
            .size = @intCast(chunk),
            .crc = fill_crc,
            .data = fill_data,
            .req_id = nextReqId(),
        };
        var resp = roundtrip(stream, &write_req, allocator) catch return;
        resp.deinit();
        if (resp.result_code != proto.OP_OK) return;
        fill_off += @intCast(chunk);
    }

    // Prepare write data
    const data = allocator.alloc(u8, size) catch return;
    defer allocator.free(data);
    @memset(data, 0xAB);
    const data_crc = crc32.hash(data);

    const max_read_off = fill_size - @as(u64, @intCast(size));
    var write_off: i64 = 0;
    var read_off: u64 = 0;
    var iterations: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        // Write
        const write_req = Packet{
            .opcode = proto.OP_WRITE,
            .partition_id = PARTITION_ID,
            .extent_id = write_eid,
            .extent_offset = write_off,
            .size = @intCast(size),
            .crc = data_crc,
            .data = data,
            .req_id = nextReqId(),
        };
        var wresp = roundtrip(stream, &write_req, allocator) catch return;
        wresp.deinit();
        write_off += @intCast(size);

        // Read
        const read_req = Packet{
            .opcode = proto.OP_STREAM_READ,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = PARTITION_ID,
            .extent_id = read_eid,
            .extent_offset = @intCast(read_off),
            .size = @intCast(size),
            .req_id = nextReqId(),
        };
        var rresp = roundtrip(stream, &read_req, allocator) catch return;
        rresp.deinit();

        read_off = (read_off + size) % max_read_off;
        iterations += 1;

        if (iterations % 32 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    printResult("BenchmarkDataNode_MixedWorkload_64KB", iterations, elapsed, @intCast(2 * size));
}

// ─── Benchmark: Watermarks / Metadata ───────────────────────────────

fn benchGetWatermarks(allocator: std.mem.Allocator) void {
    const stream = connectToServer() catch {
        std.debug.print("benchGetWatermarks: cannot connect to server\n", .{});
        return;
    };
    defer stream.close();

    var iterations: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        const req = Packet{
            .opcode = proto.OP_GET_ALL_WATERMARKS,
            .result_code = proto.OP_INIT_RESULT_CODE,
            .partition_id = PARTITION_ID,
            .req_id = nextReqId(),
        };

        var resp = roundtrip(stream, &req, allocator) catch return;
        resp.deinit();

        iterations += 1;

        if (iterations % 64 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    printLatencyResult("BenchmarkDataNode_GetWatermarks", iterations, elapsed);
}

// ─── CLI parsing ────────────────────────────────────────────────────

fn parseArgs() void {
    const args = std.process.args();
    var iter = args;
    _ = iter.next(); // skip argv[0]
    while (iter.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--port=")) {
            SERVER_PORT = std.fmt.parseInt(u16, arg[7..], 10) catch SERVER_PORT;
        } else if (std.mem.startsWith(u8, arg, "--host=")) {
            SERVER_HOST = arg[7..];
        } else if (std.mem.startsWith(u8, arg, "--partition=")) {
            PARTITION_ID = std.fmt.parseInt(u64, arg[12..], 10) catch PARTITION_ID;
        }
    }
}

// ─── Main ───────────────────────────────────────────────────────────

pub fn main() !void {
    const allocator = std.heap.c_allocator;

    parseArgs();

    const stdout = std.io.getStdOut().writer();
    try stdout.print("goos: linux\n", .{});
    try stdout.print("goarch: amd64\n", .{});
    try stdout.print("pkg: cubefs/datanode-net\n", .{});
    try stdout.print("target: {s}:{d} partition={d}\n", .{ SERVER_HOST, SERVER_PORT, PARTITION_ID });
    try stdout.print("\n", .{});

    // Verify connectivity
    {
        const stream = connectToServer() catch |e| {
            try stdout.print("ERROR: cannot connect to {s}:{d}: {}\n", .{ SERVER_HOST, SERVER_PORT, e });
            try stdout.print("Please start the datanode server first.\n", .{});
            return;
        };
        stream.close();
        try stdout.print("Connected to {s}:{d} OK\n\n", .{ SERVER_HOST, SERVER_PORT });
    }

    // Create extent latency
    benchCreateExtent(allocator);

    // Sequential writes
    benchSeqWrite(allocator, 4 * 1024);
    benchSeqWrite(allocator, 64 * 1024);
    benchSeqWrite(allocator, 128 * 1024);

    // Sequential reads
    benchSeqRead(allocator, 4 * 1024);
    benchSeqRead(allocator, 64 * 1024);
    benchSeqRead(allocator, 128 * 1024);

    // Concurrent writes (4 connections)
    benchConcurrentWrite(allocator, 64 * 1024, 4);

    // Mixed workload
    benchMixed(allocator, 64 * 1024);

    // Metadata queries
    benchGetWatermarks(allocator);

    try stdout.print("PASS\n", .{});
}
