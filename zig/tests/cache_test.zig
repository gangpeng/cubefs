// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

const std = @import("std");
const testing = std.testing;
const engine = @import("cubefs_engine");
const cache_mod = engine._extent_cache;
const extent_mod = engine._extent;

const ExtentCache = cache_mod.ExtentCache;
const Extent = extent_mod.Extent;

fn makeExtent(allocator: std.mem.Allocator, path_buf: []u8, id: u64) !*Extent {
    const path = std.fmt.bufPrintZ(path_buf, "/tmp/zig_cache_test_extent_{d}_{d}", .{ id, std.time.milliTimestamp() }) catch unreachable;
    return try Extent.create(allocator, path, id);
}

test "basic put get" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 10);
    defer {
        cache.clear();
        cache.deinit();
    }

    var path_buf: [256]u8 = undefined;
    const ext = try makeExtent(allocator, &path_buf, 1024);
    defer std.fs.cwd().deleteFile(ext.file_path) catch {};

    cache.put(1024, ext);

    const got = cache.get(1024);
    try testing.expect(got != null);
    try testing.expectEqual(@as(u64, 1024), got.?.extent_id);
}

test "remove extent from cache" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 10);
    defer {
        cache.clear();
        cache.deinit();
    }

    var path_buf: [256]u8 = undefined;
    const ext = try makeExtent(allocator, &path_buf, 2000);
    defer {
        std.fs.cwd().deleteFile(ext.file_path) catch {};
        ext.destroy();
    }

    cache.put(2000, ext);
    try testing.expect(cache.get(2000) != null);

    _ = cache.remove(2000);
    try testing.expect(cache.get(2000) == null);
}

test "tiny extent never evicted" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 1);
    defer {
        cache.clear();
        cache.deinit();
    }

    var path_buf1: [256]u8 = undefined;
    var path_buf2: [256]u8 = undefined;
    var path_buf3: [256]u8 = undefined;

    const tiny = try makeExtent(allocator, &path_buf1, 1);
    defer std.fs.cwd().deleteFile(tiny.file_path) catch {};
    cache.put(1, tiny);

    const normal1 = try makeExtent(allocator, &path_buf2, 1024);
    // normal1 will be evicted when normal2 is inserted, so the cache will destroy it
    defer std.fs.cwd().deleteFile(normal1.file_path) catch {};
    cache.put(1024, normal1);

    const normal2 = try makeExtent(allocator, &path_buf3, 1025);
    // normal2 remains in cache, cache.clear() will destroy it
    defer std.fs.cwd().deleteFile(normal2.file_path) catch {};
    cache.put(1025, normal2);

    try testing.expect(cache.get(1) != null);
    try testing.expect(cache.get(1025) != null);
}

test "cache count tracking" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 10);
    defer {
        cache.clear();
        cache.deinit();
    }

    try testing.expectEqual(@as(usize, 0), cache.normalCount());
    try testing.expectEqual(@as(usize, 0), cache.tinyCount());

    var path_buf1: [256]u8 = undefined;
    var path_buf2: [256]u8 = undefined;

    // Add a normal extent
    const normal = try makeExtent(allocator, &path_buf1, 1024);
    defer std.fs.cwd().deleteFile(normal.file_path) catch {};
    cache.put(1024, normal);
    try testing.expectEqual(@as(usize, 1), cache.normalCount());

    // Add a tiny extent (ID <= 64)
    const tiny = try makeExtent(allocator, &path_buf2, 5);
    defer std.fs.cwd().deleteFile(tiny.file_path) catch {};
    cache.put(5, tiny);
    try testing.expectEqual(@as(usize, 1), cache.tinyCount());
}

test "eviction happens at capacity" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 2);
    defer {
        cache.clear();
        cache.deinit();
    }

    var path_buf1: [256]u8 = undefined;
    var path_buf2: [256]u8 = undefined;
    var path_buf3: [256]u8 = undefined;

    const ext1 = try makeExtent(allocator, &path_buf1, 1100);
    defer std.fs.cwd().deleteFile(ext1.file_path) catch {};
    cache.put(1100, ext1);

    const ext2 = try makeExtent(allocator, &path_buf2, 1101);
    defer std.fs.cwd().deleteFile(ext2.file_path) catch {};
    cache.put(1101, ext2);

    try testing.expectEqual(@as(usize, 2), cache.normalCount());

    // Access ext2 to make it more recently used
    _ = cache.get(1101);

    // Adding ext3 should evict the least recently used (ext1)
    const ext3 = try makeExtent(allocator, &path_buf3, 1102);
    defer std.fs.cwd().deleteFile(ext3.file_path) catch {};
    cache.put(1102, ext3);

    // One should have been evicted, count should still be 2
    try testing.expectEqual(@as(usize, 2), cache.normalCount());
}

test "clear removes all entries" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 10);
    defer cache.deinit();

    var path_buf1: [256]u8 = undefined;
    var path_buf2: [256]u8 = undefined;

    // Save paths before extent could be freed
    var saved_path1: [256]u8 = undefined;
    var saved_path2: [256]u8 = undefined;
    var path1_len: usize = 0;
    var path2_len: usize = 0;

    const ext1 = try makeExtent(allocator, &path_buf1, 2000);
    path1_len = ext1.file_path.len;
    @memcpy(saved_path1[0..path1_len], ext1.file_path);
    cache.put(2000, ext1);

    const tiny = try makeExtent(allocator, &path_buf2, 3);
    path2_len = tiny.file_path.len;
    @memcpy(saved_path2[0..path2_len], tiny.file_path);
    cache.put(3, tiny);

    cache.clear();

    // Clean up files using saved paths
    defer std.fs.cwd().deleteFile(saved_path1[0..path1_len]) catch {};
    defer std.fs.cwd().deleteFile(saved_path2[0..path2_len]) catch {};

    try testing.expectEqual(@as(usize, 0), cache.normalCount());
    try testing.expectEqual(@as(usize, 0), cache.tinyCount());
    try testing.expect(cache.get(2000) == null);
    try testing.expect(cache.get(3) == null);
}

test "get nonexistent returns null" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 10);
    defer cache.deinit();

    try testing.expect(cache.get(9999) == null);
    try testing.expect(cache.get(1) == null); // tiny range
}

test "put same id replaces entry" {
    const allocator = testing.allocator;
    var cache = ExtentCache.init(allocator, 10);
    defer {
        cache.clear();
        cache.deinit();
    }

    var path_buf1: [256]u8 = undefined;
    var path_buf2: [256]u8 = undefined;

    const ext1 = try makeExtent(allocator, &path_buf1, 3000);
    defer std.fs.cwd().deleteFile(ext1.file_path) catch {};
    cache.put(3000, ext1);

    const ext2 = try makeExtent(allocator, &path_buf2, 3001);
    defer std.fs.cwd().deleteFile(ext2.file_path) catch {};

    // Put ext2 under the same key (3000) — should replace
    cache.put(3000, ext2);

    // Only 1 entry (replaced, not added)
    try testing.expectEqual(@as(usize, 1), cache.normalCount());

    // The returned extent should be ext2 (the replacement)
    const got = cache.get(3000);
    try testing.expect(got != null);
    try testing.expectEqual(@as(u64, 3001), got.?.extent_id);

    // ext1 is orphaned — destroy manually (cache no longer owns it)
    ext1.destroy();
}
