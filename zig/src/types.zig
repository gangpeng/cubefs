// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Core data types mirroring Go/Rust struct definitions.

const constants = @import("constants.zig");

/// Extent metadata — mirrors Go `storage.ExtentInfo`.
pub const ExtentInfo = struct {
    file_id: u64 = 0,
    size: u64 = 0,
    crc: u32 = 0,
    is_deleted: bool = false,
    modify_time: i64 = 0,
    snapshot_data_off: u64 = 0,
    apply_id: u64 = 0,

    /// Total size accounting for snapshot data beyond EXTENT_SIZE.
    pub fn totalSize(self: ExtentInfo) u64 {
        if (self.snapshot_data_off > constants.EXTENT_SIZE) {
            return self.size + (self.snapshot_data_off - constants.EXTENT_SIZE);
        }
        return self.size;
    }
};

/// C-compatible extent info struct for FFI.
pub const CExtentInfo = extern struct {
    file_id: u64 = 0,
    size: u64 = 0,
    crc: u32 = 0,
    is_deleted: bool = false,
    modify_time: i64 = 0,
    snapshot_data_off: u64 = 0,
    apply_id: u64 = 0,
};

/// Write operation parameters.
pub const WriteParam = struct {
    extent_id: u64,
    offset: i64,
    size: i64,
    data: []const u8,
    crc: u32,
    write_type: i32,
    is_sync: bool,
};

/// Convert ExtentInfo to CExtentInfo for FFI.
pub fn toCExtentInfo(info: ExtentInfo) CExtentInfo {
    return CExtentInfo{
        .file_id = info.file_id,
        .size = info.size,
        .crc = info.crc,
        .is_deleted = info.is_deleted,
        .modify_time = info.modify_time,
        .snapshot_data_off = info.snapshot_data_off,
        .apply_id = info.apply_id,
    };
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = @import("std").testing;

test "ExtentInfo default values" {
    const info = ExtentInfo{};
    try testing.expectEqual(@as(u64, 0), info.file_id);
    try testing.expectEqual(@as(u64, 0), info.size);
    try testing.expectEqual(@as(u32, 0), info.crc);
    try testing.expect(!info.is_deleted);
    try testing.expectEqual(@as(i64, 0), info.modify_time);
    try testing.expectEqual(@as(u64, 0), info.snapshot_data_off);
    try testing.expectEqual(@as(u64, 0), info.apply_id);
}

test "ExtentInfo.totalSize without snapshot overflow" {
    const info = ExtentInfo{ .size = 1000, .snapshot_data_off = 0 };
    try testing.expectEqual(@as(u64, 1000), info.totalSize());

    // snapshot_data_off within EXTENT_SIZE
    const info2 = ExtentInfo{ .size = 2000, .snapshot_data_off = constants.EXTENT_SIZE - 1 };
    try testing.expectEqual(@as(u64, 2000), info2.totalSize());

    // exactly at EXTENT_SIZE
    const info3 = ExtentInfo{ .size = 3000, .snapshot_data_off = constants.EXTENT_SIZE };
    try testing.expectEqual(@as(u64, 3000), info3.totalSize());
}

test "ExtentInfo.totalSize with snapshot overflow" {
    // snapshot_data_off > EXTENT_SIZE should add the overflow
    const overflow: u64 = 4096;
    const info = ExtentInfo{
        .size = 5000,
        .snapshot_data_off = constants.EXTENT_SIZE + overflow,
    };
    try testing.expectEqual(@as(u64, 5000 + overflow), info.totalSize());
}

test "toCExtentInfo preserves all fields" {
    const info = ExtentInfo{
        .file_id = 42,
        .size = 65536,
        .crc = 0xDEADBEEF,
        .is_deleted = true,
        .modify_time = 1700000000,
        .snapshot_data_off = 100,
        .apply_id = 99,
    };
    const c_info = toCExtentInfo(info);
    try testing.expectEqual(@as(u64, 42), c_info.file_id);
    try testing.expectEqual(@as(u64, 65536), c_info.size);
    try testing.expectEqual(@as(u32, 0xDEADBEEF), c_info.crc);
    try testing.expect(c_info.is_deleted);
    try testing.expectEqual(@as(i64, 1700000000), c_info.modify_time);
    try testing.expectEqual(@as(u64, 100), c_info.snapshot_data_off);
    try testing.expectEqual(@as(u64, 99), c_info.apply_id);
}

test "WriteParam struct layout" {
    const data = "hello";
    const param = WriteParam{
        .extent_id = 10,
        .offset = 4096,
        .size = 5,
        .data = data,
        .crc = 0x12345678,
        .write_type = constants.WRITE_TYPE_APPEND,
        .is_sync = true,
    };
    try testing.expectEqual(@as(u64, 10), param.extent_id);
    try testing.expectEqual(@as(i64, 4096), param.offset);
    try testing.expectEqual(@as(i64, 5), param.size);
    try testing.expectEqual(@as(usize, 5), param.data.len);
    try testing.expectEqual(@as(u32, 0x12345678), param.crc);
    try testing.expectEqual(constants.WRITE_TYPE_APPEND, param.write_type);
    try testing.expect(param.is_sync);
}
