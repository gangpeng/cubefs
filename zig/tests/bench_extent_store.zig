// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Benchmark suite for Zig ExtentStore — mirrors Go bench_test.go exactly.
//!
//! Output format matches Go benchmark output:
//!   BenchmarkExtentStore_SeqWrite_4KB      N    xxx ns/op    xxx MB/s
//!
//! Build: zig build bench
//! Run:   ./zig-out/bin/bench-extent-store

const std = @import("std");
const engine = @import("cubefs_engine");

const ExtentStore = engine._extent_store.ExtentStore;
const WriteParam = engine._types.WriteParam;
const constants = engine._constants;
const crc32 = engine._crc32;

/// Benchmark duration target in nanoseconds (3 seconds, matching Go -benchtime=3s).
const BENCH_DURATION_NS: u64 = 3 * std.time.ns_per_s;

/// Go's BlockSize = 128KB (max write chunk for AppendWriteType).
const GO_BLOCK_SIZE: usize = 128 * 1024;

/// Extent size threshold for rotation — use Go's 128MB to match.
const EXTENT_SIZE_THRESHOLD: u64 = 128 * 1024 * 1024;

/// Fill size for read benchmarks (10 MB, matching Go).
const READ_FILL_SIZE: u64 = 10 * 1024 * 1024;

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

// ─── Temp dir helper ────────────────────────────────────────────────

extern fn mkdtemp(template: [*:0]u8) ?[*:0]u8;

fn makeTempDir() ![4096]u8 {
    var buf: [4096]u8 = undefined;
    const template = "/tmp/zig_bench_XXXXXX";
    @memcpy(buf[0..template.len], template);
    buf[template.len] = 0;

    const ptr: [*:0]u8 = @ptrCast(&buf);
    const result = mkdtemp(ptr);
    if (result == null) return error.TempDirFailed;
    return buf;
}

fn tempDirPath(buf: *const [4096]u8) [:0]const u8 {
    const len = std.mem.indexOfScalar(u8, buf, 0) orelse buf.len;
    return buf[0..len :0];
}

// ─── Store helpers ──────────────────────────────────────────────────

fn newStore(path: [:0]const u8, partition_id: u64) !*ExtentStore {
    return ExtentStore.create(std.heap.c_allocator, path, partition_id) catch |e| {
        std.debug.print("Failed to create store: {}\n", .{e});
        return error.StoreCreateFailed;
    };
}

/// Create an extent and fill it with data (chunked at GO_BLOCK_SIZE to match Go).
fn createAndFillExtent(store: *ExtentStore, fill_size: u64) !u64 {
    const eid = store.nextExtentId();
    try store.createExtent(eid);

    var chunk: [GO_BLOCK_SIZE]u8 = undefined;
    @memset(&chunk, 0xAB);
    const chunk_crc = crc32.hash(&chunk);

    var off: u64 = 0;
    while (off < fill_size) {
        var write_len = @as(u64, GO_BLOCK_SIZE);
        if (off + write_len > fill_size) {
            write_len = fill_size - off;
        }
        if (off + write_len > EXTENT_SIZE_THRESHOLD) break;

        const param = WriteParam{
            .extent_id = eid,
            .offset = @intCast(off),
            .size = @intCast(write_len),
            .data = chunk[0..@intCast(write_len)],
            .crc = chunk_crc,
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = false,
        };
        try store.writeExtent(&param);
        off += write_len;
    }
    return eid;
}

// ─── Sequential Write ───────────────────────────────────────────────

fn benchSeqWrite(size: usize) void {
    var dir_buf = makeTempDir() catch {
        std.debug.print("benchSeqWrite: failed to create temp dir\n", .{});
        return;
    };
    const path = tempDirPath(&dir_buf);
    const store = newStore(path, 1) catch return;
    defer {
        store.destroy();
        std.fs.cwd().deleteTree(path) catch {};
    }

    var eid = store.nextExtentId();
    store.createExtent(eid) catch return;

    var data_buf: [GO_BLOCK_SIZE]u8 = undefined;
    @memset(data_buf[0..size], 0xAB);
    const data_slice = data_buf[0..size];
    const data_crc = crc32.hash(data_slice);

    var offset: u64 = 0;
    var iterations: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        // Rotate extent when approaching threshold
        if (offset + size > EXTENT_SIZE_THRESHOLD) {
            eid = store.nextExtentId();
            store.createExtent(eid) catch return;
            offset = 0;
        }

        const param = WriteParam{
            .extent_id = eid,
            .offset = @intCast(offset),
            .size = @intCast(size),
            .data = data_slice,
            .crc = data_crc,
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = false,
        };
        store.writeExtent(&param) catch return;
        offset += size;
        iterations += 1;

        if (iterations % 256 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    const name = switch (size) {
        4 * 1024 => "BenchmarkExtentStore_SeqWrite_4KB",
        64 * 1024 => "BenchmarkExtentStore_SeqWrite_64KB",
        128 * 1024 => "BenchmarkExtentStore_SeqWrite_128KB",
        else => "BenchmarkExtentStore_SeqWrite_?",
    };
    printResult(name, iterations, elapsed, @intCast(size));
}

// ─── Sequential Read ────────────────────────────────────────────────

fn benchSeqRead(alloc: std.mem.Allocator, size: usize) void {
    var dir_buf = makeTempDir() catch {
        std.debug.print("benchSeqRead: failed to create temp dir\n", .{});
        return;
    };
    const path = tempDirPath(&dir_buf);
    const store = newStore(path, 2) catch return;
    defer {
        store.destroy();
        std.fs.cwd().deleteTree(path) catch {};
    }

    const eid = createAndFillExtent(store, READ_FILL_SIZE) catch return;

    const buf = alloc.alloc(u8, size) catch return;
    defer alloc.free(buf);

    var max_off = READ_FILL_SIZE - @as(u64, @intCast(size));
    if (max_off == 0) max_off = 1;

    var read_off: u64 = 0;
    var iterations: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        _ = store.readExtent(eid, read_off, buf) catch return;
        read_off = (read_off + size) % max_off;
        iterations += 1;

        if (iterations % 256 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    const name = switch (size) {
        4 * 1024 => "BenchmarkExtentStore_SeqRead_4KB",
        64 * 1024 => "BenchmarkExtentStore_SeqRead_64KB",
        1024 * 1024 => "BenchmarkExtentStore_SeqRead_1MB",
        else => "BenchmarkExtentStore_SeqRead_?",
    };
    printResult(name, iterations, elapsed, @intCast(size));
}

// ─── Concurrent Write ───────────────────────────────────────────────

const ConcWriteShared = struct {
    store: *ExtentStore,
    size: usize,
    data: []const u8,
    data_crc: u32,
    stop: std.atomic.Value(bool),
    total_ops: std.atomic.Value(u64),
};

const WriterState = struct {
    eid: u64,
    offset: u64,
    mu: std.Thread.Mutex,
};

fn concWriteWorker(shared: *ConcWriteShared, state: *WriterState) void {
    while (!shared.stop.load(.acquire)) {
        state.mu.lock();

        // Rotate extent if needed
        if (state.offset + shared.size > EXTENT_SIZE_THRESHOLD) {
            const new_eid = shared.store.nextExtentId();
            shared.store.createExtent(new_eid) catch {
                state.mu.unlock();
                continue;
            };
            state.eid = new_eid;
            state.offset = 0;
        }

        const param = WriteParam{
            .extent_id = state.eid,
            .offset = @intCast(state.offset),
            .size = @intCast(shared.size),
            .data = shared.data,
            .crc = shared.data_crc,
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = false,
        };
        const write_result = shared.store.writeExtent(&param);
        state.offset += shared.size;
        state.mu.unlock();

        if (write_result) |_| {
            _ = shared.total_ops.fetchAdd(1, .monotonic);
        } else |_| {}
    }
}

fn benchConcurrentWrite(size: usize, clients: usize) void {
    var dir_buf = makeTempDir() catch {
        std.debug.print("benchConcurrentWrite: failed to create temp dir\n", .{});
        return;
    };
    const path = tempDirPath(&dir_buf);
    const store = newStore(path, 3) catch return;
    defer {
        store.destroy();
        std.fs.cwd().deleteTree(path) catch {};
    }

    // Create per-thread writer states
    var states: [4]WriterState = undefined;
    for (0..clients) |i| {
        const eid = store.nextExtentId();
        store.createExtent(eid) catch return;
        states[i] = WriterState{
            .eid = eid,
            .offset = 0,
            .mu = std.Thread.Mutex{},
        };
    }

    var data_buf: [GO_BLOCK_SIZE]u8 = undefined;
    @memset(data_buf[0..size], 0xAB);
    const data_slice = data_buf[0..size];
    const data_crc = crc32.hash(data_slice);

    var shared = ConcWriteShared{
        .store = store,
        .size = size,
        .data = data_slice,
        .data_crc = data_crc,
        .stop = std.atomic.Value(bool).init(false),
        .total_ops = std.atomic.Value(u64).init(0),
    };

    // Spawn worker threads
    var threads: [4]std.Thread = undefined;
    for (0..clients) |i| {
        threads[i] = std.Thread.spawn(.{}, concWriteWorker, .{ &shared, &states[i] }) catch return;
    }

    // Let them run for BENCH_DURATION_NS
    var timer = std.time.Timer.start() catch unreachable;
    while (timer.read() < BENCH_DURATION_NS) {
        std.time.sleep(1_000_000); // 1ms poll
    }
    const elapsed = timer.read();

    // Signal stop and join
    shared.stop.store(true, .release);
    for (0..clients) |i| {
        threads[i].join();
    }

    const total_ops = shared.total_ops.load(.acquire);
    const name = switch (size) {
        64 * 1024 => "BenchmarkExtentStore_ConcurrentWrite4x_64KB",
        128 * 1024 => "BenchmarkExtentStore_ConcurrentWrite4x_128KB",
        else => "BenchmarkExtentStore_ConcurrentWrite4x_?",
    };
    printResult(name, total_ops, elapsed, @intCast(size));
}

// ─── Mixed Workload ─────────────────────────────────────────────────

fn benchMixed(alloc: std.mem.Allocator, size: usize) void {
    var dir_buf = makeTempDir() catch {
        std.debug.print("benchMixed: failed to create temp dir\n", .{});
        return;
    };
    const path = tempDirPath(&dir_buf);
    const store = newStore(path, 4) catch return;
    defer {
        store.destroy();
        std.fs.cwd().deleteTree(path) catch {};
    }

    // Write extent
    var write_eid = store.nextExtentId();
    store.createExtent(write_eid) catch return;

    // Read extent — pre-fill
    const read_eid = createAndFillExtent(store, READ_FILL_SIZE) catch return;

    var data_buf: [GO_BLOCK_SIZE]u8 = undefined;
    @memset(data_buf[0..size], 0xCD);
    const data_slice = data_buf[0..size];
    const data_crc = crc32.hash(data_slice);

    const read_buf = alloc.alloc(u8, size) catch return;
    defer alloc.free(read_buf);

    var max_read_off = READ_FILL_SIZE - @as(u64, @intCast(size));
    if (max_read_off == 0) max_read_off = 1;

    var write_off: u64 = 0;
    var read_off: u64 = 0;
    var iterations: u64 = 0;

    var timer = std.time.Timer.start() catch unreachable;

    while (true) {
        // Rotate write extent if needed
        if (write_off + size > EXTENT_SIZE_THRESHOLD) {
            write_eid = store.nextExtentId();
            store.createExtent(write_eid) catch return;
            write_off = 0;
        }

        // Write
        const write_param = WriteParam{
            .extent_id = write_eid,
            .offset = @intCast(write_off),
            .size = @intCast(size),
            .data = data_slice,
            .crc = data_crc,
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = false,
        };
        store.writeExtent(&write_param) catch return;
        write_off += size;

        // Read
        _ = store.readExtent(read_eid, read_off, read_buf) catch return;
        read_off = (read_off + size) % max_read_off;

        iterations += 1;
        if (iterations % 128 == 0) {
            if (timer.read() >= BENCH_DURATION_NS) break;
        }
    }

    const elapsed = timer.read();
    printResult("BenchmarkExtentStore_MixedWorkload_64KB", iterations, elapsed, @intCast(2 * size));
}

// ─── Main ───────────────────────────────────────────────────────────

pub fn main() !void {
    const alloc = std.heap.c_allocator;

    const stdout = std.io.getStdOut().writer();
    try stdout.print("goos: linux\n", .{});
    try stdout.print("goarch: amd64\n", .{});
    try stdout.print("pkg: cubefs-zig/storage\n", .{});
    try stdout.print("cpu: Zig ExtentStore Benchmark\n", .{});

    // Sequential writes
    benchSeqWrite(4 * 1024);
    benchSeqWrite(64 * 1024);
    benchSeqWrite(128 * 1024);

    // Sequential reads
    benchSeqRead(alloc, 4 * 1024);
    benchSeqRead(alloc, 64 * 1024);
    benchSeqRead(alloc, 1024 * 1024);

    // Concurrent writes
    benchConcurrentWrite(64 * 1024, 4);
    benchConcurrentWrite(128 * 1024, 4);

    // Mixed workload
    benchMixed(alloc, 64 * 1024);

    try stdout.print("PASS\n", .{});
}
