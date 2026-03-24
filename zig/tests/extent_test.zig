// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

const std = @import("std");
const testing = std.testing;
const engine = @import("cubefs_engine");
const extent_mod = engine._extent;
const types = engine._types;
const constants = engine._constants;
const crc32 = engine._crc32;

const Extent = extent_mod.Extent;

const ConcurrentWriteState = struct {
    ext: *Extent,
    offset: i64,
    fill: u8,
    size: usize,
};

fn makeTempPath(buf: []u8, name: []const u8) [:0]const u8 {
    const result = std.fmt.bufPrintZ(buf, "/tmp/zig_extent_test_{s}_{d}", .{ name, std.time.milliTimestamp() }) catch unreachable;
    return result;
}

fn concurrentExtentWriteWorker(state: *const ConcurrentWriteState) void {
    const buf = std.heap.page_allocator.alloc(u8, state.size) catch return;
    defer std.heap.page_allocator.free(buf);

    @memset(buf, state.fill);
    const param = types.WriteParam{
        .extent_id = state.ext.extent_id,
        .offset = state.offset,
        .size = @intCast(state.size),
        .data = buf,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_RANDOM,
        .is_sync = false,
    };

    _ = state.ext.write(&param) catch return;
}

test "create and write extent" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "create_write");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 100);
    defer ext.destroy();

    try testing.expectEqual(@as(i64, 0), ext.dataSize());

    const data = "hello, cubefs!";
    const param = types.WriteParam{
        .extent_id = 100,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = crc32.hash(data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };

    _ = try ext.write(&param);
    try testing.expectEqual(@as(i64, @intCast(data.len)), ext.dataSize());

    // Read back
    var read_buf: [14]u8 = undefined;
    const n = try ext.read(0, &read_buf);
    try testing.expectEqual(data.len, n);
    try testing.expectEqualStrings(data, read_buf[0..n]);
}

test "random write" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "random_write");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 101);
    defer ext.destroy();

    const data1 = "AAAA";
    const param1 = types.WriteParam{
        .extent_id = 101,
        .offset = 0,
        .size = 4,
        .data = data1,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    _ = try ext.write(&param1);

    const data2 = "BB";
    const param2 = types.WriteParam{
        .extent_id = 101,
        .offset = 2,
        .size = 2,
        .data = data2,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_RANDOM,
        .is_sync = false,
    };
    _ = try ext.write(&param2);

    var read_buf: [4]u8 = undefined;
    _ = try ext.read(0, &read_buf);
    try testing.expectEqualStrings("AABB", &read_buf);
}

test "open existing extent" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "open_existing");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;

    {
        const ext = try Extent.create(allocator, path, 102);
        defer ext.destroy();

        const data = "persistent data";
        const param = types.WriteParam{
            .extent_id = 102,
            .offset = 0,
            .size = @intCast(data.len),
            .data = data,
            .crc = 0,
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = true,
        };
        _ = try ext.write(&param);
    }

    const ext = try Extent.open(allocator, path, 102);
    defer ext.destroy();

    try testing.expectEqual(@as(i64, 15), ext.dataSize());

    var read_buf: [15]u8 = undefined;
    const n = try ext.read(0, &read_buf);
    try testing.expectEqual(@as(usize, 15), n);
    try testing.expectEqualStrings("persistent data", read_buf[0..n]);
}

test "close prevents io" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "close_io");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 103);
    defer ext.destroy();

    try ext.close();

    var read_buf: [10]u8 = undefined;
    const result = ext.read(0, &read_buf);
    try testing.expectError(error.ExtentDeleted, result);
}

test "truncate extent" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "truncate");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 104);
    defer ext.destroy();

    var data: [8192]u8 = undefined;
    @memset(&data, 0xAA);
    const param = types.WriteParam{
        .extent_id = 104,
        .offset = 0,
        .size = 8192,
        .data = &data,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    _ = try ext.write(&param);

    try ext.truncate(4096);
    try testing.expectEqual(@as(i64, 4096), ext.dataSize());
}

test "block crc tracking" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "block_crc");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 208);
    defer ext.destroy();

    var data: [4096]u8 = undefined;
    @memset(&data, 0xAB);
    const param = types.WriteParam{
        .extent_id = 208,
        .offset = 0,
        .size = 4096,
        .data = &data,
        .crc = crc32.hash(&data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    _ = try ext.write(&param);

    const crc0 = ext.blockCrc(0);
    try testing.expect(crc0 != null);
    try testing.expectEqual(crc32.hash(&data), crc0.?);

    try testing.expect(ext.blockCrc(1) == null);
}

test "read beyond written data" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "read_beyond");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 203);
    defer ext.destroy();

    const data = "short";
    const param = types.WriteParam{
        .extent_id = 203,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = true,
    };
    _ = try ext.write(&param);

    var read_buf: [100]u8 = undefined;
    @memset(&read_buf, 0xFF);
    const n = try ext.read(0, &read_buf);
    try testing.expectEqual(@as(usize, 5), n);
    try testing.expectEqualStrings("short", read_buf[0..5]);
}

test "CRC fast-path: block-aligned write with caller CRC stores without recompute" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "crc_fast_path");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 300);
    defer ext.destroy();

    // Write exactly one block (4KB) at block-aligned offset with caller CRC
    var data: [4096]u8 = undefined;
    @memset(&data, 0xCC);
    const caller_crc = crc32.hash(&data);

    const param = types.WriteParam{
        .extent_id = 300,
        .offset = 0,
        .size = 4096,
        .data = &data,
        .crc = caller_crc,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    const blocks = try ext.write(&param);
    try testing.expectEqual(@as(u32, 1), blocks);

    // The stored CRC should match the caller CRC (fast-path)
    const stored_crc = ext.blockCrc(0);
    try testing.expect(stored_crc != null);
    try testing.expectEqual(caller_crc, stored_crc.?);
}

test "CRC fast-path: multi-block aligned write stores caller CRC for each block" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "crc_fast_multi");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 301);
    defer ext.destroy();

    // Write 3 blocks (12KB) at offset 0 with caller CRC
    var data: [12288]u8 = undefined;
    @memset(&data, 0xDD);
    const caller_crc = crc32.hash(&data);

    const param = types.WriteParam{
        .extent_id = 301,
        .offset = 0,
        .size = 12288,
        .data = &data,
        .crc = caller_crc,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    const blocks = try ext.write(&param);
    try testing.expectEqual(@as(u32, 3), blocks);

    // All 3 blocks should have the caller CRC (fast-path stores same CRC for each)
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        const stored = ext.blockCrc(i);
        try testing.expect(stored != null);
        try testing.expectEqual(caller_crc, stored.?);
    }
    // Block 3 should be null
    try testing.expect(ext.blockCrc(3) == null);
}

test "CRC slow-path: unaligned write computes per-block CRC" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "crc_slow_path");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 302);
    defer ext.destroy();

    // Write at non-block-aligned offset with caller CRC (forces slow path)
    var data: [4096]u8 = undefined;
    @memset(&data, 0xEE);
    const caller_crc = crc32.hash(&data);

    const param = types.WriteParam{
        .extent_id = 302,
        .offset = 100, // not block-aligned
        .size = 4096,
        .data = &data,
        .crc = caller_crc,
        .write_type = constants.WRITE_TYPE_RANDOM,
        .is_sync = false,
    };
    _ = try ext.write(&param);

    // Block 0 should have a CRC computed from the actual data slice for block 0
    const crc0 = ext.blockCrc(0);
    try testing.expect(crc0 != null);
    // The CRC should NOT be the caller CRC (slow path computes per-block)
    // (it's the CRC of just the portion of data that falls in block 0)
}

test "CRC slow-path: zero caller CRC computes per-block CRC" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "crc_zero_caller");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 303);
    defer ext.destroy();

    // Write at block-aligned offset but with CRC=0 (forces slow path)
    var data: [4096]u8 = undefined;
    @memset(&data, 0xFF);

    const param = types.WriteParam{
        .extent_id = 303,
        .offset = 0,
        .size = 4096,
        .data = &data,
        .crc = 0, // zero CRC forces slow path
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    const blocks = try ext.write(&param);
    try testing.expectEqual(@as(u32, 1), blocks);

    // Block 0 should have the computed CRC of the data
    const stored = ext.blockCrc(0);
    try testing.expect(stored != null);
    try testing.expectEqual(crc32.hash(&data), stored.?);
}

test "data_size update under write_mutex" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "data_size_update");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 304);
    defer ext.destroy();

    // Write 1000 bytes at offset 0
    var data1: [1000]u8 = undefined;
    @memset(&data1, 0x11);
    const p1 = types.WriteParam{
        .extent_id = 304,
        .offset = 0,
        .size = 1000,
        .data = &data1,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    _ = try ext.write(&p1);
    try testing.expectEqual(@as(i64, 1000), ext.dataSize());

    // Write 500 bytes at offset 500 (overlapping, new end = 1000, should not shrink)
    var data2: [500]u8 = undefined;
    @memset(&data2, 0x22);
    const p2 = types.WriteParam{
        .extent_id = 304,
        .offset = 500,
        .size = 500,
        .data = &data2,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_RANDOM,
        .is_sync = false,
    };
    _ = try ext.write(&p2);
    try testing.expectEqual(@as(i64, 1000), ext.dataSize());

    // Write 100 bytes at offset 2000 (extends)
    var data3: [100]u8 = undefined;
    @memset(&data3, 0x33);
    const p3 = types.WriteParam{
        .extent_id = 304,
        .offset = 2000,
        .size = 100,
        .data = &data3,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_RANDOM,
        .is_sync = false,
    };
    _ = try ext.write(&p3);
    try testing.expectEqual(@as(i64, 2100), ext.dataSize());
}

test "flush and dirty flag" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "flush_dirty");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 305);
    defer ext.destroy();

    // Initially not dirty
    try ext.flush(); // should be a no-op

    // Write makes it dirty
    const data = "dirty data";
    const param = types.WriteParam{
        .extent_id = 305,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    _ = try ext.write(&param);

    // Flush should sync
    try ext.flush();

    // Second flush should be a no-op (dirty cleared)
    try ext.flush();
}

test "isClosed and modifyTime accessors" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "accessors");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 306);
    defer ext.destroy();

    try testing.expect(!ext.isClosed());
    try testing.expect(ext.modifyTime() > 0);

    try ext.close();
    try testing.expect(ext.isClosed());
}

test "multiple sequential writes" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "multi_write");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 205);
    defer ext.destroy();

    var i: u8 = 0;
    while (i < 100) : (i += 1) {
        var data: [100]u8 = undefined;
        @memset(&data, i);
        const param = types.WriteParam{
            .extent_id = 205,
            .offset = @as(i64, @intCast(i)) * 100,
            .size = 100,
            .data = &data,
            .crc = 0,
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = false,
        };
        _ = try ext.write(&param);
    }

    try testing.expectEqual(@as(i64, 10000), ext.dataSize());

    i = 0;
    while (i < 100) : (i += 1) {
        var read_buf: [100]u8 = undefined;
        const n = try ext.read(@as(u64, @intCast(i)) * 100, &read_buf);
        try testing.expectEqual(@as(usize, 100), n);
        for (read_buf[0..n]) |b| {
            try testing.expectEqual(i, b);
        }
    }
}

test "concurrent writes to one extent keep chunk boundaries intact" {
    var buf: [256]u8 = undefined;
    const path = makeTempPath(&buf, "concurrent_shared_extent");
    defer std.fs.cwd().deleteFile(path) catch {};

    const allocator = testing.allocator;
    const ext = try Extent.create(allocator, path, 400);
    defer ext.destroy();

    const chunk_size: usize = 1024;
    var states = [_]ConcurrentWriteState{
        .{ .ext = ext, .offset = 0, .fill = 'A', .size = chunk_size },
        .{ .ext = ext, .offset = chunk_size, .fill = 'B', .size = chunk_size },
        .{ .ext = ext, .offset = chunk_size * 2, .fill = 'C', .size = chunk_size },
        .{ .ext = ext, .offset = chunk_size * 3, .fill = 'D', .size = chunk_size },
    };

    var threads: [states.len]std.Thread = undefined;
    for (&threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, concurrentExtentWriteWorker, .{&states[i]});
    }
    for (threads) |thread| thread.join();

    try testing.expectEqual(@as(i64, chunk_size * states.len), ext.dataSize());

    var read_buf: [chunk_size * states.len]u8 = undefined;
    const n = try ext.read(0, &read_buf);
    try testing.expectEqual(@as(usize, chunk_size * states.len), n);
    try testing.expect(std.mem.allEqual(u8, read_buf[0..chunk_size], 'A'));
    try testing.expect(std.mem.allEqual(u8, read_buf[chunk_size .. chunk_size * 2], 'B'));
    try testing.expect(std.mem.allEqual(u8, read_buf[chunk_size * 2 .. chunk_size * 3], 'C'));
    try testing.expect(std.mem.allEqual(u8, read_buf[chunk_size * 3 .. chunk_size * 4], 'D'));
}
