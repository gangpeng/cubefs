// io_uring_test.zig — Tests for the io_uring async I/O engine.
//
// Tests single read, single write, fsync, batch operations, and error
// handling using real file I/O through io_uring.

const std = @import("std");
const testing = std.testing;
const posix = std.posix;
const engine = @import("cubefs_engine");

const UringEngine = engine._io_uring_engine.UringEngine;
const IoCompletion = engine._io_uring_engine.IoCompletion;

// ─── Helpers ──────────────────────────────────────────────────────

fn makeTempFile(data: []const u8) !struct { fd: posix.fd_t, path: []u8 } {
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const path = try std.fmt.allocPrint(testing.allocator, "/tmp/zig_uring_test_{d}", .{timestamp});
    errdefer testing.allocator.free(path);

    const file = try std.fs.cwd().createFile(path, .{ .read = true });
    if (data.len > 0) {
        try file.writeAll(data);
        try file.seekTo(0);
    }
    return .{ .fd = file.handle, .path = path };
}

fn removeTempFile(path: []u8) void {
    std.fs.cwd().deleteFile(path) catch {};
    testing.allocator.free(path);
}

fn initUringOrSkip() !UringEngine {
    return UringEngine.init() catch |e| switch (e) {
        error.SystemResources, error.SystemOutdated, error.PermissionDenied => return error.SkipZigTest,
        else => return e,
    };
}

fn initUringWithDepthOrSkip(depth: u16) !UringEngine {
    return UringEngine.initWithDepth(depth) catch |e| switch (e) {
        error.SystemResources, error.SystemOutdated, error.PermissionDenied => return error.SkipZigTest,
        else => return e,
    };
}

// ─── Init/Deinit Tests ────────────────────────────────────────────

test "io_uring: init and deinit" {
    var uring = try initUringOrSkip();
    uring.deinit();
}

test "io_uring: init with custom depth" {
    var uring = try initUringWithDepthOrSkip(64);
    uring.deinit();
}

// ─── Single Write Tests ───────────────────────────────────────────

test "io_uring: single write" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const tmp = try makeTempFile("");
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    const data = "hello io_uring write";
    const written = try uring.write(tmp.fd, 0, data);
    try testing.expectEqual(data.len, written);
}

// ─── Single Read Tests ────────────────────────────────────────────

test "io_uring: write then read" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const write_data = "io_uring read test data";
    const tmp = try makeTempFile(write_data);
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    var buf: [64]u8 = undefined;
    const n = try uring.read(tmp.fd, 0, buf[0..write_data.len]);
    try testing.expectEqual(write_data.len, n);
    try testing.expectEqualStrings(write_data, buf[0..n]);
}

test "io_uring: read at offset" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const write_data = "AAAA_BBBB_CCCC";
    const tmp = try makeTempFile(write_data);
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    var buf: [4]u8 = undefined;
    const n = try uring.read(tmp.fd, 5, &buf);
    try testing.expectEqual(@as(usize, 4), n);
    try testing.expectEqualStrings("BBBB", &buf);
}

// ─── Fsync Tests ──────────────────────────────────────────────────

test "io_uring: fsync after write" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const tmp = try makeTempFile("");
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    _ = try uring.write(tmp.fd, 0, "fsync test data");
    try uring.fsync(tmp.fd);
}

// ─── Write then Read roundtrip ────────────────────────────────────

test "io_uring: write and read roundtrip" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const tmp = try makeTempFile("");
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    const data = "roundtrip via io_uring";
    _ = try uring.write(tmp.fd, 0, data);
    try uring.fsync(tmp.fd);

    var buf: [64]u8 = undefined;
    const n = try uring.read(tmp.fd, 0, buf[0..data.len]);
    try testing.expectEqual(data.len, n);
    try testing.expectEqualStrings(data, buf[0..n]);
}

// ─── Multiple sequential operations ──────────────────────────────

test "io_uring: multiple sequential writes and reads" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const tmp = try makeTempFile("");
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    // Write 4 chunks
    const chunks = [_][]const u8{ "AAAA", "BBBB", "CCCC", "DDDD" };
    for (chunks, 0..) |chunk, i| {
        _ = try uring.write(tmp.fd, @intCast(i * 4), chunk);
    }
    try uring.fsync(tmp.fd);

    // Read back each chunk
    for (chunks, 0..) |expected, i| {
        var buf: [4]u8 = undefined;
        _ = try uring.read(tmp.fd, @intCast(i * 4), &buf);
        try testing.expectEqualStrings(expected, &buf);
    }
}

// ─── Large I/O Tests ──────────────────────────────────────────────

test "io_uring: write and read 64KB" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const tmp = try makeTempFile("");
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    // Write 64KB
    const size = 64 * 1024;
    var data: [size]u8 = undefined;
    @memset(&data, 0xAB);

    _ = try uring.write(tmp.fd, 0, &data);
    try uring.fsync(tmp.fd);

    var read_buf: [size]u8 = undefined;
    const n = try uring.read(tmp.fd, 0, &read_buf);
    try testing.expectEqual(@as(usize, size), n);
    try testing.expectEqualSlices(u8, &data, &read_buf);
}

test "io_uring: invalid fd operations return IoError" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    var buf: [8]u8 = undefined;
    try testing.expectError(error.IoError, uring.read(-1, 0, &buf));
    try testing.expectError(error.IoError, uring.write(-1, 0, "badfd"));
    try testing.expectError(error.IoError, uring.fsync(-1));
}

test "io_uring: read past EOF returns 0 bytes" {
    var uring = try initUringWithDepthOrSkip(1);
    defer uring.deinit();

    const tmp = try makeTempFile("");
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    var buf: [8]u8 = undefined;
    const n = try uring.read(tmp.fd, 1024, &buf);
    try testing.expectEqual(@as(usize, 0), n);
}

const ConcurrentWriteState = struct {
    uring: *UringEngine,
    fd: posix.fd_t,
    offset: u64,
    ch: u8,
};

fn concurrentWriteWorker(state: *const ConcurrentWriteState) void {
    var data: [1024]u8 = undefined;
    @memset(&data, state.ch);
    _ = state.uring.write(state.fd, state.offset, &data) catch return;
}

test "io_uring: concurrent writes are serialized safely" {
    var uring = try initUringOrSkip();
    defer uring.deinit();

    const tmp = try makeTempFile("");
    defer {
        posix.close(tmp.fd);
        removeTempFile(tmp.path);
    }

    var states = [_]ConcurrentWriteState{
        .{ .uring = &uring, .fd = tmp.fd, .offset = 0, .ch = 'A' },
        .{ .uring = &uring, .fd = tmp.fd, .offset = 1024, .ch = 'B' },
        .{ .uring = &uring, .fd = tmp.fd, .offset = 2048, .ch = 'C' },
        .{ .uring = &uring, .fd = tmp.fd, .offset = 3072, .ch = 'D' },
    };

    var threads: [states.len]std.Thread = undefined;
    for (&threads, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, concurrentWriteWorker, .{&states[i]});
    }
    for (threads) |t| t.join();

    var read_back: [4096]u8 = undefined;
    const n = try uring.read(tmp.fd, 0, &read_back);
    try testing.expectEqual(@as(usize, 4096), n);

    try testing.expect(std.mem.allEqual(u8, read_back[0..1024], 'A'));
    try testing.expect(std.mem.allEqual(u8, read_back[1024..2048], 'B'));
    try testing.expect(std.mem.allEqual(u8, read_back[2048..3072], 'C'));
    try testing.expect(std.mem.allEqual(u8, read_back[3072..4096], 'D'));
}
