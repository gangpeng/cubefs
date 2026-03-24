// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

const std = @import("std");
const testing = std.testing;
const engine = @import("cubefs_engine");
const extent_store_mod = engine._extent_store;
const types = engine._types;
const constants = engine._constants;
const crc32 = engine._crc32;

const ExtentStore = extent_store_mod.ExtentStore;

fn makeTempDir(buf: []u8) [:0]const u8 {
    const result = std.fmt.bufPrintZ(buf, "/tmp/zig_store_test_{d}", .{std.time.milliTimestamp()}) catch unreachable;
    return result;
}

fn cleanupDir(path: [:0]const u8) void {
    std.fs.cwd().deleteTree(path) catch {};
}

test "create and write extent via store" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    const data = "hello extent store";
    const param = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = crc32.hash(data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };

    try store.writeExtent(&param);

    var read_buf: [18]u8 = undefined;
    const result = try store.readExtent(eid, 0, &read_buf);
    try testing.expectEqual(data.len, result.bytes_read);
    try testing.expectEqualStrings(data, read_buf[0..result.bytes_read]);
    try testing.expectEqual(crc32.hash(data), result.crc);
}

test "snapshot returns all" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const eid = store.nextExtentId();
        try store.createExtent(eid);
    }

    const snap = try store.snapshot(allocator);
    defer allocator.free(snap);
    try testing.expectEqual(@as(usize, 5), snap.len);
}

test "mark delete" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    try store.markDelete(eid);

    const info = store.getExtentInfo(eid);
    try testing.expect(info != null);
    try testing.expect(info.?.is_deleted);
}

test "read nonexistent extent" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    var read_buf: [100]u8 = undefined;
    const result = store.readExtent(99999, 0, &read_buf);
    try testing.expectError(error.ExtentNotFound, result);
}

test "create duplicate extent" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);
    const result = store.createExtent(eid);
    try testing.expectError(error.ExtentExists, result);
}

test "has extent check" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try testing.expect(!store.hasExtent(eid));
    try store.createExtent(eid);
    try testing.expect(store.hasExtent(eid));
}

test "used size tracks writes" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    try testing.expectEqual(@as(u64, 0), store.usedSize());

    var data: [1024]u8 = undefined;
    @memset(&data, 0xAB);
    const param = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = 1024,
        .data = &data,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    try store.writeExtent(&param);

    try testing.expectEqual(@as(u64, 1024), store.usedSize());
}

test "tiny extent pool" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    try testing.expect(store.putAvailableTinyExtent(1));
    try testing.expect(store.putAvailableTinyExtent(2));

    try testing.expectEqual(@as(?u64, 1), store.getAvailableTinyExtent());
    try testing.expectEqual(@as(?u64, 2), store.getAvailableTinyExtent());
    try testing.expectEqual(@as(?u64, null), store.getAvailableTinyExtent());
}

test "next extent id monotonic" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    var prev: u64 = 0;
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const id = store.nextExtentId();
        try testing.expect(id > prev or prev == 0);
        prev = id;
    }
}

test "watermarks exclude deleted" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid1 = store.nextExtentId();
    const eid2 = store.nextExtentId();
    const eid3 = store.nextExtentId();
    try store.createExtent(eid1);
    try store.createExtent(eid2);
    try store.createExtent(eid3);

    try store.markDelete(eid2);

    const watermarks = try store.getAllWatermarks(allocator);
    defer allocator.free(watermarks);
    try testing.expectEqual(@as(usize, 2), watermarks.len);
}

test "flush all extents" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const eid = store.nextExtentId();
        try store.createExtent(eid);
        var data: [100]u8 = undefined;
        @memset(&data, 0xAB);
        const param = types.WriteParam{
            .extent_id = eid,
            .offset = 0,
            .size = 100,
            .data = &data,
            .crc = 0,
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = false,
        };
        try store.writeExtent(&param);
    }

    try store.flush();
}

test "apply_id get and set" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    try testing.expectEqual(@as(u64, 0), store.applyId());
    store.setApplyId(42);
    try testing.expectEqual(@as(u64, 42), store.applyId());
    store.setApplyId(100);
    try testing.expectEqual(@as(u64, 100), store.applyId());
}

test "write updates metadata in shard map" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    // Info should have size 0 initially
    const info1 = store.getExtentInfo(eid);
    try testing.expect(info1 != null);
    try testing.expectEqual(@as(u64, 0), info1.?.size);

    // Write some data
    var data: [2048]u8 = undefined;
    @memset(&data, 0xBB);
    const param = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = 2048,
        .data = &data,
        .crc = crc32.hash(&data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    try store.writeExtent(&param);

    // Info should now reflect the write
    const info2 = store.getExtentInfo(eid);
    try testing.expect(info2 != null);
    try testing.expectEqual(@as(u64, 2048), info2.?.size);
    try testing.expectEqual(crc32.hash(&data), info2.?.crc);
    try testing.expect(info2.?.modify_time > 0);
}

test "read with CRC verification" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    const data = "verify crc on read";
    const param = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = crc32.hash(data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = true,
    };
    try store.writeExtent(&param);

    var read_buf: [100]u8 = undefined;
    const result = try store.readExtent(eid, 0, &read_buf);
    try testing.expectEqual(data.len, result.bytes_read);
    try testing.expectEqualStrings(data, read_buf[0..result.bytes_read]);
    // The CRC from readExtent is computed on the actually-read data
    try testing.expectEqual(crc32.hash(data), result.crc);
}

test "readNoCrc returns data without store-level CRC" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    const data = "no crc stream read";
    const param = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = 0,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = true,
    };
    try store.writeExtent(&param);

    var read_buf: [100]u8 = undefined;
    const n = try store.readNoCrc(eid, 0, &read_buf);
    try testing.expectEqual(data.len, n);
    try testing.expectEqualStrings(data, read_buf[0..n]);
}

test "close store prevents further operations" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    store.close();

    // Attempting to create/write/read should fail
    const create_result = store.createExtent(eid + 1);
    try testing.expectError(error.ExtentDeleted, create_result);

    var read_buf: [100]u8 = undefined;
    const read_result = store.readExtent(eid, 0, &read_buf);
    try testing.expectError(error.ExtentDeleted, read_result);
}

test "extent count tracks creates and deletes" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    try testing.expectEqual(@as(usize, 0), store.extentCount());

    const eid1 = store.nextExtentId();
    const eid2 = store.nextExtentId();
    try store.createExtent(eid1);
    try store.createExtent(eid2);
    try testing.expectEqual(@as(usize, 2), store.extentCount());

    // markDelete doesn't remove from count (just sets is_deleted)
    try store.markDelete(eid1);
    try testing.expectEqual(@as(usize, 2), store.extentCount());
}

test "GC delete lock blocks writes until unlock" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    const locks = [_]extent_store_mod.ExtentLock{
        .{ .extent_id = eid, .flag = .gc_delete },
    };
    try store.batchLockExtents(&locks);
    try testing.expectEqual(extent_store_mod.GcFlag.gc_delete, store.getExtentLock(eid).?);

    const data = "locked write";
    const blocked = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = crc32.hash(data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    try testing.expectError(error.ExtentLocked, store.writeExtent(&blocked));

    store.batchUnlockExtents();
    try testing.expectEqual(@as(?extent_store_mod.GcFlag, null), store.getExtentLock(eid));

    const allowed = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = crc32.hash(data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    try store.writeExtent(&allowed);
}

test "persisted apply ID survives store restart" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;

    {
        const store = try ExtentStore.create(allocator, dir, 1);
        defer store.destroy();
        store.setApplyId(777);
        try store.persistApplyId();
    }

    {
        const reloaded = try ExtentStore.create(allocator, dir, 1);
        defer reloaded.destroy();
        try testing.expectEqual(@as(u64, 777), reloaded.applyId());
    }
}

test "loadExistingExtents rebuilds metadata and next ID" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    var e1: u64 = 0;
    var e2: u64 = 0;

    {
        const store = try ExtentStore.create(allocator, dir, 1);
        defer store.destroy();

        e1 = store.nextExtentId();
        e2 = store.nextExtentId();
        try store.createExtent(e1);
        try store.createExtent(e2);

        const data = "persist me";
        const p = types.WriteParam{
            .extent_id = e1,
            .offset = 0,
            .size = @intCast(data.len),
            .data = data,
            .crc = crc32.hash(data),
            .write_type = constants.WRITE_TYPE_APPEND,
            .is_sync = true,
        };
        try store.writeExtent(&p);
    }

    {
        const reloaded = try ExtentStore.create(allocator, dir, 1);
        defer reloaded.destroy();

        try testing.expect(reloaded.hasExtent(e1));
        try testing.expect(reloaded.hasExtent(e2));
        try testing.expectEqual(@as(usize, 2), reloaded.extentCount());

        const info = reloaded.getExtentInfo(e1);
        try testing.expect(info != null);
        try testing.expectEqual(@as(u64, 10), info.?.size);

        const next = reloaded.nextExtentId();
        try testing.expect(next > e1);
        try testing.expect(next > e2);
    }
}

test "punchDelete appends tiny delete record" {
    var buf: [256]u8 = undefined;
    const dir = makeTempDir(&buf);
    defer cleanupDir(dir);

    const allocator = testing.allocator;
    const store = try ExtentStore.create(allocator, dir, 1);
    defer store.destroy();

    const eid = store.nextExtentId();
    try store.createExtent(eid);

    var data: [4096]u8 = undefined;
    @memset(&data, 0xCC);
    const p = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = data.len,
        .data = &data,
        .crc = crc32.hash(&data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = true,
    };
    try store.writeExtent(&p);

    store.punchDelete(eid, 512, 256) catch |e| switch (e) {
        error.IoError => return error.SkipZigTest,
        else => return e,
    };

    const records = try store.readTinyDeleteRecords(allocator);
    defer if (records.len > 0) allocator.free(records);

    try testing.expect(records.len >= 1);
    const r = records[records.len - 1];
    try testing.expectEqual(eid, r.extent_id);
    try testing.expectEqual(@as(u64, 512), r.offset);
    try testing.expectEqual(@as(u64, 256), r.size);
}
