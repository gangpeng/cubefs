// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

const std = @import("std");
const testing = std.testing;
const engine = @import("cubefs_engine");
const disk_mod = engine._disk;
const partition_mod = engine._partition;
const space_mgr_mod = engine._space_manager;
const types = engine._types;
const constants = engine._constants;
const crc32 = engine._crc32;

const Disk = disk_mod.Disk;
const DataPartition = partition_mod.DataPartition;
const SpaceManager = space_mgr_mod.SpaceManager;

fn makeTempDir(buf: []u8, label: []const u8) [:0]const u8 {
    return std.fmt.bufPrintZ(buf, "/tmp/zig_dp_test_{s}_{d}", .{ label, std.time.milliTimestamp() }) catch unreachable;
}

fn cleanupDir(path: [:0]const u8) void {
    std.fs.cwd().deleteTree(path) catch {};
}

// ─── Disk Tests ─────────────────────────────────────────────────────

test "Disk.init creates directory and reads space" {
    const allocator = testing.allocator;
    var buf: [256]u8 = undefined;
    const path = makeTempDir(&buf, "disk_init");
    defer cleanupDir(path);

    const disk = try Disk.init(allocator, path, 0, 0, 0);
    defer disk.deinit();

    // Total should be > 0 (any real filesystem)
    try testing.expect(disk.getTotal() > 0);
    try testing.expect(disk.getAvailable() > 0);
    try testing.expect(disk.isGood());
}

test "Disk.hasSpace accounts for reserved space" {
    const allocator = testing.allocator;
    var buf: [256]u8 = undefined;
    const path = makeTempDir(&buf, "disk_reserved");
    defer cleanupDir(path);

    // Reserve a huge amount so hasSpace returns false
    const disk = try Disk.init(allocator, path, std.math.maxInt(u64), 0, 0);
    defer disk.deinit();

    try testing.expect(!disk.hasSpace(1));
}

test "Disk.hasSpace with no reserved space" {
    const allocator = testing.allocator;
    var buf: [256]u8 = undefined;
    const path = makeTempDir(&buf, "disk_no_reserve");
    defer cleanupDir(path);

    const disk = try Disk.init(allocator, path, 0, 0, 0);
    defer disk.deinit();

    // Should have space for 1 byte
    try testing.expect(disk.hasSpace(1));
}

test "Disk.setStatus and isGood" {
    const allocator = testing.allocator;
    var buf: [256]u8 = undefined;
    const path = makeTempDir(&buf, "disk_status");
    defer cleanupDir(path);

    const disk = try Disk.init(allocator, path, 0, 0, 0);
    defer disk.deinit();

    try testing.expect(disk.isGood());

    disk.setStatus(disk_mod.DISK_STATUS_BAD);
    try testing.expect(!disk.isGood());

    disk.setStatus(disk_mod.DISK_STATUS_GOOD);
    try testing.expect(disk.isGood());
}

test "Disk.updateSpace refreshes statistics" {
    const allocator = testing.allocator;
    var buf: [256]u8 = undefined;
    const path = makeTempDir(&buf, "disk_update");
    defer cleanupDir(path);

    const disk = try Disk.init(allocator, path, 0, 0, 0);
    defer disk.deinit();

    const before_total = disk.getTotal();
    try disk.updateSpace();
    const after_total = disk.getTotal();

    // Total should remain consistent
    try testing.expectEqual(before_total, after_total);
}

test "Disk.checkDiskError marks disk bad only at threshold" {
    const allocator = testing.allocator;
    var buf: [256]u8 = undefined;
    const path = makeTempDir(&buf, "disk_err_threshold");
    defer cleanupDir(path);

    const disk = try Disk.init(allocator, path, 0, 0, 0);
    defer disk.deinit();

    disk.max_err_cnt = 2;

    disk.checkDiskError(true);
    try testing.expectEqual(@as(u64, 1), disk.getReadErrCnt());
    try testing.expectEqual(@as(u64, 0), disk.getWriteErrCnt());
    try testing.expect(disk.isGood());

    disk.checkDiskError(true);
    try testing.expectEqual(@as(u64, 2), disk.getReadErrCnt());
    try testing.expect(!disk.isGood());
}

// ─── DataPartition Tests ────────────────────────────────────────────

test "DataPartition.init creates store directory" {
    const allocator = testing.allocator;
    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "dp_disk");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    var dp_buf: [256]u8 = undefined;
    const dp_path = std.fmt.bufPrintZ(&dp_buf, "{s}/dp_1", .{disk_path}) catch unreachable;

    const dp = try DataPartition.init(allocator, 1, 100, dp_path, &.{}, disk);
    defer dp.deinit();

    try testing.expectEqual(@as(u64, 1), dp.partition_id);
    try testing.expectEqual(@as(u64, 100), dp.volume_id);
    try testing.expect(dp.isNormal());
}

test "DataPartition status transitions" {
    const allocator = testing.allocator;
    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "dp_status");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    var dp_buf: [256]u8 = undefined;
    const dp_path = std.fmt.bufPrintZ(&dp_buf, "{s}/dp_2", .{disk_path}) catch unreachable;

    const dp = try DataPartition.init(allocator, 2, 200, dp_path, &.{}, disk);
    defer dp.deinit();

    try testing.expectEqual(partition_mod.PARTITION_STATUS_READ_WRITE, dp.getStatus());
    try testing.expect(dp.isNormal());

    dp.setStatus(partition_mod.PARTITION_STATUS_READ_ONLY);
    try testing.expectEqual(partition_mod.PARTITION_STATUS_READ_ONLY, dp.getStatus());
    try testing.expect(!dp.isNormal());

    dp.setStatus(partition_mod.PARTITION_STATUS_UNAVAILABLE);
    try testing.expect(!dp.isNormal());
}

test "DataPartition size and applied_id" {
    const allocator = testing.allocator;
    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "dp_size");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    var dp_buf: [256]u8 = undefined;
    const dp_path = std.fmt.bufPrintZ(&dp_buf, "{s}/dp_3", .{disk_path}) catch unreachable;

    const dp = try DataPartition.init(allocator, 3, 300, dp_path, &.{}, disk);
    defer dp.deinit();

    try testing.expectEqual(@as(u64, 0), dp.getSize());
    dp.setSize(1024 * 1024);
    try testing.expectEqual(@as(u64, 1024 * 1024), dp.getSize());

    try testing.expectEqual(@as(u64, 0), dp.getAppliedId());
    dp.setAppliedId(42);
    try testing.expectEqual(@as(u64, 42), dp.getAppliedId());
}

test "DataPartition write and read through store" {
    const allocator = testing.allocator;
    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "dp_io");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    var dp_buf: [256]u8 = undefined;
    const dp_path = std.fmt.bufPrintZ(&dp_buf, "{s}/dp_4", .{disk_path}) catch unreachable;

    const dp = try DataPartition.init(allocator, 4, 400, dp_path, &.{}, disk);
    defer dp.deinit();

    // Create an extent through the store
    const eid = dp.store.nextExtentId();
    try dp.store.createExtent(eid);

    try testing.expectEqual(@as(usize, 1), dp.extentCount());
    try testing.expectEqual(@as(u64, 0), dp.usedSize());

    // Write data
    const data = "partition write test";
    const param = types.WriteParam{
        .extent_id = eid,
        .offset = 0,
        .size = @intCast(data.len),
        .data = data,
        .crc = crc32.hash(data),
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = false,
    };
    try dp.store.writeExtent(&param);

    try testing.expectEqual(@as(u64, data.len), dp.usedSize());
}

// ─── SpaceManager Tests ─────────────────────────────────────────────

test "SpaceManager.init and empty state" {
    const allocator = testing.allocator;
    var mgr = SpaceManager.init(allocator);
    defer mgr.deinit();

    try testing.expectEqual(@as(usize, 0), mgr.diskCount());
    try testing.expectEqual(@as(usize, 0), mgr.partitionCount());
    try testing.expect(mgr.selectDisk() == null);
}

test "SpaceManager.addDisk and getDisk" {
    const allocator = testing.allocator;
    var mgr = SpaceManager.init(allocator);
    defer mgr.deinit();

    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "mgr_disk");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    try mgr.addDisk(disk);
    try testing.expectEqual(@as(usize, 1), mgr.diskCount());

    const got = mgr.getDisk(0);
    try testing.expect(got != null);

    // Out-of-range returns null
    try testing.expect(mgr.getDisk(100) == null);
}

test "SpaceManager.selectDisk picks good disk with most space" {
    const allocator = testing.allocator;
    var mgr = SpaceManager.init(allocator);
    defer mgr.deinit();

    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "mgr_select");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    try mgr.addDisk(disk);

    const selected = mgr.selectDisk();
    try testing.expect(selected != null);
}

test "SpaceManager.selectDisk skips bad disks" {
    const allocator = testing.allocator;
    var mgr = SpaceManager.init(allocator);
    defer mgr.deinit();

    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "mgr_bad_disk");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    disk.setStatus(disk_mod.DISK_STATUS_BAD);
    try mgr.addDisk(disk);

    const selected = mgr.selectDisk();
    try testing.expect(selected == null);
}

test "SpaceManager partition operations" {
    const allocator = testing.allocator;
    var mgr = SpaceManager.init(allocator);
    defer mgr.deinit();

    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "mgr_parts");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    // Create partition directly
    var dp_buf: [256]u8 = undefined;
    const dp_path = std.fmt.bufPrintZ(&dp_buf, "{s}/dp_10", .{disk_path}) catch unreachable;

    const dp = try DataPartition.init(allocator, 10, 1000, dp_path, &.{}, disk);

    mgr.addPartition(dp);
    try testing.expectEqual(@as(usize, 1), mgr.partitionCount());
    try testing.expect(mgr.getPartition(10) != null);
    try testing.expect(mgr.getPartition(99) == null);

    // Remove
    const removed = mgr.removePartition(10);
    try testing.expect(removed != null);
    removed.?.deinit();

    try testing.expectEqual(@as(usize, 0), mgr.partitionCount());
    try testing.expect(mgr.getPartition(10) == null);
}

test "SpaceManager.allPartitionIds and allPartitions" {
    const allocator = testing.allocator;
    var mgr = SpaceManager.init(allocator);
    defer mgr.deinit();

    var disk_buf: [256]u8 = undefined;
    const disk_path = makeTempDir(&disk_buf, "mgr_all");
    defer cleanupDir(disk_path);

    const disk = try Disk.init(allocator, disk_path, 0, 0, 0);
    defer disk.deinit();

    // Create 3 partitions
    var dp_bufs: [3][256]u8 = undefined;
    var dps: [3]*DataPartition = undefined;
    for (0..3) |i| {
        const dp_path = std.fmt.bufPrintZ(&dp_bufs[i], "{s}/dp_{d}", .{ disk_path, i + 20 }) catch unreachable;
        dps[i] = try DataPartition.init(allocator, @intCast(i + 20), 500, dp_path, &.{}, disk);
        mgr.addPartition(dps[i]);
    }

    // Deferred cleanup
    defer {
        for (0..3) |i| {
            if (mgr.removePartition(@intCast(i + 20))) |dp| {
                dp.deinit();
            }
        }
    }

    const ids = try mgr.allPartitionIds(allocator);
    defer allocator.free(ids);
    try testing.expectEqual(@as(usize, 3), ids.len);

    const parts = try mgr.allPartitions(allocator);
    defer allocator.free(parts);
    try testing.expectEqual(@as(usize, 3), parts.len);
}

test "SpaceManager.partitionPath generates correct format" {
    var buf: [256]u8 = undefined;
    const path = try SpaceManager.partitionPath("/mnt/disk1", 42, &buf);
    try testing.expectEqualStrings("/mnt/disk1/datapartition_42", path);
}
