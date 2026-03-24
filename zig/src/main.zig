// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! CubeFS Zig Engine — FFI exports for Go/Java/Python consumption.
//!
//! Produces `libcubefs_engine.so` with `export` + `callconv(.c)` functions
//! that match the exact same C header as the Rust implementation.

const std = @import("std");
const constants = @import("constants.zig");
const err = @import("error.zig");
const types = @import("types.zig");
const extent_store_mod = @import("extent_store.zig");

const ExtentStore = extent_store_mod.ExtentStore;
const ExtentInfo = types.ExtentInfo;
const CExtentInfo = types.CExtentInfo;
const WriteParam = types.WriteParam;

/// Global allocator for FFI — uses the general purpose allocator.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = gpa.allocator();

// =============================================================================
// Extent Store FFI
// =============================================================================

/// Create a new extent store at the given directory path.
export fn cubefs_extent_store_new(data_path: [*:0]const u8, partition_id: u64) ?*ExtentStore {
    const path = std.mem.span(data_path);
    return ExtentStore.create(allocator, path, partition_id) catch null;
}

/// Destroy an extent store, flushing and closing all extents.
export fn cubefs_extent_store_free(store: ?*ExtentStore) void {
    if (store) |s| {
        s.close();
        s.destroy();
    }
}

/// Create a new extent in the store.
export fn cubefs_extent_create(store: ?*ExtentStore, extent_id: u64) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;
    s.createExtent(extent_id) catch |e| return err.toFfiCode(e);
    return constants.CUBEFS_OK;
}

/// Allocate the next extent ID.
export fn cubefs_extent_next_id(store: ?*ExtentStore) u64 {
    const s = store orelse return 0;
    return s.nextExtentId();
}

/// Write data to an extent.
export fn cubefs_extent_write(
    store: ?*ExtentStore,
    extent_id: u64,
    offset: i64,
    data: [*]const u8,
    data_len: usize,
    crc: u32,
    write_type: i32,
    is_sync: bool,
) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;

    if (data_len > 0 and @intFromPtr(data) == 0) {
        return constants.CUBEFS_ERR_INVALID_ARG;
    }

    const data_slice = if (data_len > 0) data[0..data_len] else &[_]u8{};

    const param = WriteParam{
        .extent_id = extent_id,
        .offset = offset,
        .size = @intCast(data_len),
        .data = data_slice,
        .crc = crc,
        .write_type = write_type,
        .is_sync = is_sync,
    };

    s.writeExtent(&param) catch |e| return err.toFfiCode(e);
    return constants.CUBEFS_OK;
}

/// Read data from an extent.
export fn cubefs_extent_read(
    store: ?*ExtentStore,
    extent_id: u64,
    offset: u64,
    buf: [*]u8,
    buf_len: usize,
    bytes_read: *usize,
    crc_out: *u32,
) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;

    const buf_slice = buf[0..buf_len];

    const result = s.readExtent(extent_id, offset, buf_slice) catch |e| return err.toFfiCode(e);

    bytes_read.* = result.bytes_read;
    crc_out.* = result.crc;
    return constants.CUBEFS_OK;
}

/// Mark an extent as deleted.
export fn cubefs_extent_delete(store: ?*ExtentStore, extent_id: u64) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;
    s.markDelete(extent_id) catch |e| return err.toFfiCode(e);
    return constants.CUBEFS_OK;
}

/// Get extent info by ID.
export fn cubefs_extent_info(store: ?*ExtentStore, extent_id: u64, info_out: ?*CExtentInfo) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;
    const out = info_out orelse return constants.CUBEFS_ERR_INVALID_ARG;

    if (s.getExtentInfo(extent_id)) |info| {
        out.* = types.toCExtentInfo(info);
        return constants.CUBEFS_OK;
    }
    return constants.CUBEFS_ERR_NOT_FOUND;
}

/// Get the number of extents in the store.
export fn cubefs_extent_count(store: ?*ExtentStore) usize {
    const s = store orelse return 0;
    return s.extentCount();
}

/// Get a snapshot of all extent info.
export fn cubefs_extent_store_snapshot(
    store: ?*ExtentStore,
    out_ptr: *?[*]CExtentInfo,
    out_len: *usize,
) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;

    const infos = s.snapshot(allocator) catch return constants.CUBEFS_ERR_INTERNAL;

    // Allocate C array
    const c_infos = allocator.alloc(CExtentInfo, infos.len) catch {
        allocator.free(infos);
        return constants.CUBEFS_ERR_INTERNAL;
    };

    for (infos, 0..) |info, i| {
        c_infos[i] = types.toCExtentInfo(info);
    }
    allocator.free(infos);

    out_ptr.* = c_infos.ptr;
    out_len.* = c_infos.len;
    return constants.CUBEFS_OK;
}

/// Get watermarks (non-deleted extent info).
export fn cubefs_extent_store_watermarks(
    store: ?*ExtentStore,
    out_ptr: *?[*]CExtentInfo,
    out_len: *usize,
) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;

    const infos = s.getAllWatermarks(allocator) catch return constants.CUBEFS_ERR_INTERNAL;

    const c_infos = allocator.alloc(CExtentInfo, infos.len) catch {
        allocator.free(infos);
        return constants.CUBEFS_ERR_INTERNAL;
    };

    for (infos, 0..) |info, i| {
        c_infos[i] = types.toCExtentInfo(info);
    }
    allocator.free(infos);

    out_ptr.* = c_infos.ptr;
    out_len.* = c_infos.len;
    return constants.CUBEFS_OK;
}

/// Free an array of CExtentInfo returned by snapshot/watermarks.
export fn cubefs_extent_infos_free(ptr: ?[*]CExtentInfo, len_val: usize) void {
    if (ptr) |p| {
        if (len_val > 0) {
            allocator.free(p[0..len_val]);
        }
    }
}

/// Check if the store has an extent with the given ID.
export fn cubefs_extent_store_has_extent(store: ?*ExtentStore, extent_id: u64) bool {
    const s = store orelse return false;
    return s.hasExtent(extent_id);
}

/// Get total used size across all non-deleted extents.
export fn cubefs_extent_store_used_size(store: ?*ExtentStore) u64 {
    const s = store orelse return 0;
    return s.usedSize();
}

/// Flush all cached extents to disk.
export fn cubefs_extent_store_flush(store: ?*ExtentStore) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;
    s.flush() catch |e| return err.toFfiCode(e);
    return constants.CUBEFS_OK;
}

/// Get an available tiny extent ID from the pool.
export fn cubefs_extent_store_get_tiny(store: ?*ExtentStore) u64 {
    const s = store orelse return 0;
    return s.getAvailableTinyExtent() orelse 0;
}

/// Return a tiny extent ID to the available pool.
export fn cubefs_extent_store_put_tiny(store: ?*ExtentStore, extent_id: u64) bool {
    const s = store orelse return false;
    return s.putAvailableTinyExtent(extent_id);
}

/// Punch a hole in a tiny extent (range deletion).
export fn cubefs_extent_mark_delete_range(
    store: ?*ExtentStore,
    extent_id: u64,
    offset: i64,
    size: i64,
) i32 {
    const s = store orelse return constants.CUBEFS_ERR_INVALID_ARG;
    s.punchDelete(extent_id, @bitCast(offset), @bitCast(size)) catch |e| return err.toFfiCode(e);
    return constants.CUBEFS_OK;
}

// =============================================================================
// Stub functions for EC encoder and buffer pool (not implemented in Zig yet)
// =============================================================================

/// Create a new EC encoder (stub — returns null).
export fn cubefs_ec_encoder_new(_data_shards: usize, _parity_shards: usize) ?*anyopaque {
    _ = _data_shards;
    _ = _parity_shards;
    return null;
}

/// Destroy an EC encoder (stub — no-op).
export fn cubefs_ec_encoder_free(_encoder: ?*anyopaque) void {
    _ = _encoder;
}

/// Create a new buffer pool (stub — returns null).
export fn cubefs_buffer_pool_new() ?*anyopaque {
    return null;
}

/// Destroy a buffer pool (stub — no-op).
export fn cubefs_buffer_pool_free(_pool: ?*anyopaque) void {
    _ = _pool;
}

// =============================================================================
// Version info
// =============================================================================

/// Get the engine version string.
export fn cubefs_engine_version() [*:0]const u8 {
    return "cubefs-engine-zig 0.1.0";
}

/// Initialize the Zig engine.
export fn cubefs_engine_init() void {
    // No-op for Zig — no runtime initialization needed.
}

// =============================================================================
// Test exports
// =============================================================================
pub const _constants = @import("constants.zig");
pub const _error = @import("error.zig");
pub const _types = @import("types.zig");
pub const _crc32 = @import("crc32.zig");
pub const _extent = @import("extent.zig");
pub const _extent_cache = @import("extent_cache.zig");
pub const _extent_store = extent_store_mod;
pub const _tiny_ring = @import("tiny_ring.zig");
pub const _sharded_map = @import("sharded_map.zig");
pub const _io_uring_engine = @import("io_uring_engine.zig");
pub const _disk = @import("disk.zig");
pub const _partition = @import("partition.zig");
pub const _space_manager = @import("space_manager.zig");
pub const _raft = @import("raft.zig");
pub const _raft_log = @import("raft_log.zig");
pub const _raft_stub = @import("raft_stub.zig");
pub const _crc_persistence = @import("crc_persistence.zig");
pub const _tiny_delete_record = @import("tiny_delete_record.zig");
pub const _apply_id_persistence = @import("apply_id_persistence.zig");
pub const _metrics = @import("metrics.zig");
pub const _protocol = @import("protocol.zig");
pub const _packet = @import("packet.zig");
pub const _codec = @import("codec.zig");
pub const _handler_io = @import("handler_io.zig");
pub const _admin = @import("admin.zig");
pub const _replication = @import("replication.zig");
pub const _json = @import("json.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
