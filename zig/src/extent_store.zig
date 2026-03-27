// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Extent store — mirrors Go `storage.ExtentStore` / Rust `extent_store.rs`.
//!
//! The master storage controller for a data partition. Uses:
//! - ShardedMap for concurrent metadata access
//! - ExtentCache for open file handle LRU caching
//! - TinyRing for tiny extent allocation

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

const constants = @import("constants.zig");
const err = @import("error.zig");
const types = @import("types.zig");
const crc32_mod = @import("crc32.zig");
const extent_mod = @import("extent.zig");
const cache_mod = @import("extent_cache.zig");
const tiny_ring_mod = @import("tiny_ring.zig");
const sharded_map_mod = @import("sharded_map.zig");
const crc_persist_mod = @import("crc_persistence.zig");
const tiny_del_mod = @import("tiny_delete_record.zig");
const apply_id_mod = @import("apply_id_persistence.zig");

const Extent = extent_mod.Extent;
const ExtentCache = cache_mod.ExtentCache;
const TinyRing = tiny_ring_mod.TinyRing;
const ExtentInfo = types.ExtentInfo;
const WriteParam = types.WriteParam;

/// Default cache capacity for normal extents.
const DEFAULT_CACHE_CAPACITY: usize = 4096;
/// Default tiny extent ring capacity.
const DEFAULT_TINY_RING_CAPACITY: usize = 128;

/// GC flag for extent locking during backup/GC.
pub const GcFlag = enum(u8) {
    normal = 0,
    gc_mark = 1,
    gc_delete = 2,
};

/// A single extent lock entry.
pub const ExtentLock = struct {
    extent_id: u64,
    flag: GcFlag,
};

pub const ExtentStore = struct {
    /// Base directory for extent files (owned, null-terminated).
    data_path: [:0]const u8,
    /// Next extent ID to allocate (atomic counter).
    base_extent_id: std.atomic.Value(u64),
    /// Concurrent hashmap of extent metadata: extent_id -> ExtentInfo.
    extent_info_map: sharded_map_mod.ShardedMap(u64, ExtentInfo),
    /// LRU cache of open extent file handles.
    cache: ExtentCache,
    /// Ring buffer of available tiny extent IDs.
    available_tiny: TinyRing,
    /// Ring buffer of broken tiny extent IDs.
    broken_tiny: TinyRing,
    /// Partition ID this store belongs to.
    partition_id: u64,
    /// Whether the store has been closed.
    closed: std.atomic.Value(bool),
    /// Latest raft apply ID.
    apply_id: std.atomic.Value(u64),
    /// Serializes extent access against lifecycle changes and cache eviction.
    /// Read operations use lockShared(); lifecycle-mutating operations use lock().
    extent_lifecycle_lock: std.Thread.RwLock,
    /// Serializes cache-miss open/insert so only one handle is created per extent.
    extent_open_mu: std.Thread.Mutex,
    /// Guards the drain wait in close/destroy.
    op_mu: std.Thread.Mutex,
    /// Notifies close/destroy when in-flight operations drain to zero.
    op_cv: std.Thread.Condition,
    /// Number of extent operations that have entered but not yet exited (atomic).
    active_ops: std.atomic.Value(u32),
    /// Whether destroy has started and new operations must be rejected.
    destroying: std.atomic.Value(bool),
    /// CRC persistence (optional, null until initialized).
    crc_persistence: ?crc_persist_mod.CrcPersistence,
    /// Tiny delete recorder (optional, null until initialized).
    tiny_delete_recorder: ?tiny_del_mod.TinyDeleteRecorder,
    /// Extent lock map for GC/backup locking.
    extent_lock_map: std.AutoHashMap(u64, GcFlag),
    /// Mutex for extent lock map.
    lock_mu: std.Thread.Mutex,
    /// Whether extent locking is enabled.
    extent_lock_enabled: std.atomic.Value(bool),
    /// Allocator.
    allocator: Allocator,

    /// Create a new extent store at the given directory path.
    pub fn create(allocator: Allocator, data_path: [:0]const u8, partition_id: u64) err.Error!*ExtentStore {
        // Create directory if it doesn't exist
        std.fs.cwd().makePath(data_path) catch return error.IoError;

        const self = allocator.create(ExtentStore) catch return error.IoError;
        self.* = ExtentStore{
            .data_path = data_path,
            .base_extent_id = std.atomic.Value(u64).init(constants.MIN_EXTENT_ID),
            .extent_info_map = sharded_map_mod.ShardedMap(u64, ExtentInfo).init(allocator),
            .cache = ExtentCache.init(allocator, DEFAULT_CACHE_CAPACITY),
            .available_tiny = TinyRing.init(allocator, DEFAULT_TINY_RING_CAPACITY) catch return error.IoError,
            .broken_tiny = TinyRing.init(allocator, DEFAULT_TINY_RING_CAPACITY) catch return error.IoError,
            .partition_id = partition_id,
            .closed = std.atomic.Value(bool).init(false),
            .apply_id = std.atomic.Value(u64).init(0),
            .extent_lifecycle_lock = .{},  // RwLock default init
            .extent_open_mu = .{},
            .op_mu = .{},
            .op_cv = .{},
            .active_ops = std.atomic.Value(u32).init(0),
            .destroying = std.atomic.Value(bool).init(false),
            .crc_persistence = null,
            .tiny_delete_recorder = null,
            .extent_lock_map = std.AutoHashMap(u64, GcFlag).init(allocator),
            .lock_mu = .{},
            .extent_lock_enabled = std.atomic.Value(bool).init(false),
            .allocator = allocator,
        };

        // Initialize CRC persistence
        self.crc_persistence = crc_persist_mod.CrcPersistence.init(allocator, data_path) catch null;

        // Initialize tiny delete recorder
        self.tiny_delete_recorder = tiny_del_mod.TinyDeleteRecorder.init(data_path) catch null;

        // Load persisted apply ID
        const persisted_id = apply_id_mod.load(data_path);
        self.apply_id.store(persisted_id, .release);

        // Scan existing extents on disk
        self.loadExistingExtents() catch return error.IoError;

        return self;
    }

    /// Destroy the extent store, freeing all resources.
    pub fn destroy(self: *ExtentStore) void {
        // Signal that we are destroying — reject new operations atomically.
        self.destroying.store(true, .release);
        self.closed.store(true, .release);

        // Wait for in-flight operations to drain.
        self.op_mu.lock();
        while (self.active_ops.load(.acquire) != 0) {
            self.op_cv.wait(&self.op_mu);
        }
        self.op_mu.unlock();

        self.extent_lifecycle_lock.lock();  // exclusive — destroying

        self.cache.clear();
        self.cache.deinit();
        self.extent_info_map.deinit();
        self.available_tiny.deinit();
        self.broken_tiny.deinit();
        if (self.crc_persistence) |*cp| cp.deinit();
        if (self.tiny_delete_recorder) |*tdr| tdr.deinit();
        self.extent_lock_map.deinit();

        self.extent_lifecycle_lock.unlock();
        self.allocator.destroy(self);
    }

    fn beginOp(self: *ExtentStore) err.Error!void {
        // Atomically check destroying/closed and increment active_ops.
        // No mutex needed on the hot path — only the drain path uses op_mu.
        if (self.destroying.load(.acquire) or self.closed.load(.acquire)) {
            return error.ExtentDeleted;
        }
        _ = self.active_ops.fetchAdd(1, .acq_rel);
        // Re-check after increment to close the race with destroy/close.
        if (self.destroying.load(.acquire) or self.closed.load(.acquire)) {
            self.decrementOps();
            return error.ExtentDeleted;
        }
    }

    fn endOp(self: *ExtentStore) void {
        self.decrementOps();
    }

    /// Decrement active_ops and wake drain waiters if it reaches zero.
    fn decrementOps(self: *ExtentStore) void {
        const prev = self.active_ops.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        if (prev == 1) {
            // Was the last op — wake any draining thread.
            self.op_mu.lock();
            self.op_cv.broadcast();
            self.op_mu.unlock();
        }
    }

    fn lookupExtentInfo(self: *ExtentStore, extent_id: u64) ?ExtentInfo {
        return self.extent_info_map.get(extent_id);
    }

    /// Create a new extent file.
    pub fn createExtent(self: *ExtentStore, extent_id: u64) err.Error!void {
        try self.beginOp();
        defer self.endOp();

        self.extent_lifecycle_lock.lock();  // exclusive — lifecycle mutation
        defer self.extent_lifecycle_lock.unlock();

        if (self.closed.load(.acquire)) {
            return error.ExtentDeleted;
        }

        if (self.extent_info_map.contains(extent_id)) {
            return error.ExtentExists;
        }

        const path = self.extentFilePath(extent_id);
        const ext = try Extent.create(self.allocator, path, extent_id);

        // Insert metadata
        const info = ExtentInfo{ .file_id = extent_id };
        self.extent_info_map.put(extent_id, info);

        // Cache the open handle
        self.cache.put(extent_id, ext);

        // Track the highest extent ID for GetMaxExtentIdAndPartitionSize
        var current = self.base_extent_id.load(.acquire);
        while (extent_id >= current) {
            const prev = self.base_extent_id.cmpxchgWeak(current, extent_id + 1, .seq_cst, .acquire);
            if (prev) |p| {
                current = p;
            } else {
                break;
            }
        }
    }

    /// Allocate the next extent ID.
    pub fn nextExtentId(self: *ExtentStore) u64 {
        return self.base_extent_id.fetchAdd(1, .seq_cst);
    }

    /// Write data to an extent.
    pub fn writeExtent(self: *ExtentStore, param: *const WriteParam) err.Error!void {
        try self.beginOp();
        defer self.endOp();

        // Phase 1 (shared lock): closed check → GC lock check → getOrOpenExtent → ext.write → read modify_time
        self.extent_lifecycle_lock.lockShared();  // shared — ext.write() has its own per-extent write_mutex

        if (self.closed.load(.acquire)) {
            self.extent_lifecycle_lock.unlockShared();
            return error.ExtentDeleted;
        }

        // Check GC lock
        if (self.extent_lock_enabled.load(.acquire)) {
            self.lock_mu.lock();
            const flag = self.extent_lock_map.get(param.extent_id);
            self.lock_mu.unlock();
            if (flag) |f| {
                if (f == .gc_delete) {
                    self.extent_lifecycle_lock.unlockShared();
                    return error.ExtentLocked;
                }
            }
        }

        // getOrOpenExtent will return ExtentNotFound if the extent doesn't exist,
        // so no need for a separate contains() check.
        const ext = self.getOrOpenExtent(param.extent_id) catch |e| {
            self.extent_lifecycle_lock.unlockShared();
            return e;
        };
        _ = ext.write(param) catch |e| {
            self.extent_lifecycle_lock.unlockShared();
            return e;
        };

        const modify_time = ext.modifyTime();
        self.extent_lifecycle_lock.unlockShared();

        // Phase 2 (no lifecycle lock): CRC persistence → shard map metadata update
        if (self.crc_persistence) |*cp| {
            if (param.offset >= 0 and param.crc != 0) {
                const block_no: u32 = @intCast(@as(u64, @intCast(param.offset)) / crc_persist_mod.CRC_BLOCK_SIZE);
                cp.persistBlockCrc(param.extent_id, block_no, param.crc) catch {};
            }
        }

        // Update metadata in the shard map (has its own per-shard RwLock).
        const new_size: u64 = @intCast(param.offset + param.size);
        const idx = sharded_map_mod.ShardedMap(u64, ExtentInfo).shardIndex(param.extent_id);
        var shard = &self.extent_info_map.shards[idx];
        shard.lock.lock();
        defer shard.lock.unlock();
        if (shard.map.getPtr(param.extent_id)) |info_ptr| {
            if (new_size > info_ptr.size) {
                info_ptr.size = new_size;
            }
            info_ptr.crc = param.crc;
            info_ptr.modify_time = modify_time;
        }
    }

    /// Read data from an extent. Returns (crc, bytes_read).
    pub fn readExtent(self: *ExtentStore, extent_id: u64, offset: u64, buf: []u8) err.Error!struct { crc: u32, bytes_read: usize } {
        try self.beginOp();
        defer self.endOp();

        self.extent_lifecycle_lock.lockShared();  // shared — read path
        defer self.extent_lifecycle_lock.unlockShared();

        if (self.closed.load(.acquire)) {
            return error.ExtentDeleted;
        }

        const ext = try self.getOrOpenExtent(extent_id);
        const n = try ext.read(offset, buf);
        const crc = crc32_mod.hash(buf[0..n]);
        return .{ .crc = crc, .bytes_read = n };
    }

    /// Mark an extent as deleted.
    pub fn markDelete(self: *ExtentStore, extent_id: u64) err.Error!void {
        try self.beginOp();
        defer self.endOp();

        self.extent_lifecycle_lock.lock();  // exclusive — lifecycle mutation
        defer self.extent_lifecycle_lock.unlock();

        // Update metadata
        const idx = sharded_map_mod.ShardedMap(u64, ExtentInfo).shardIndex(extent_id);
        var shard = &self.extent_info_map.shards[idx];
        shard.lock.lock();
        if (shard.map.getPtr(extent_id)) |info_ptr| {
            info_ptr.is_deleted = true;
            shard.lock.unlock();
        } else {
            shard.lock.unlock();
            return error.ExtentNotFound;
        }

        // Delete CRC data
        if (self.crc_persistence) |*cp| {
            cp.deleteBlockCrc(extent_id) catch {};
        }

        // Remove from cache
        if (self.cache.remove(extent_id)) |ext| {
            ext.close() catch {};
            ext.destroy();
        }

        // Delete the file
        const path = self.extentFilePath(extent_id);
        std.fs.cwd().deleteFile(path) catch {};
    }

    /// Get a snapshot of all extent metadata.
    pub fn snapshot(self: *ExtentStore, allocator: Allocator) ![]ExtentInfo {
        try self.beginOp();
        defer self.endOp();

        var list = std.ArrayList(ExtentInfo).init(allocator);
        errdefer list.deinit();

        for (&self.extent_info_map.shards) |*s| {
            s.lock.lockShared();
            defer s.lock.unlockShared();

            var it = s.map.valueIterator();
            while (it.next()) |v| {
                try list.append(v.*);
            }
        }

        return try list.toOwnedSlice();
    }

    /// Get all extent info with watermarks (non-deleted extents).
    pub fn getAllWatermarks(self: *ExtentStore, allocator: Allocator) ![]ExtentInfo {
        try self.beginOp();
        defer self.endOp();

        var list = std.ArrayList(ExtentInfo).init(allocator);
        errdefer list.deinit();

        for (&self.extent_info_map.shards) |*s| {
            s.lock.lockShared();
            defer s.lock.unlockShared();

            var it = s.map.valueIterator();
            while (it.next()) |v| {
                if (!v.is_deleted) {
                    try list.append(v.*);
                }
            }
        }

        return try list.toOwnedSlice();
    }

    /// Get extent info by ID.
    pub fn getExtentInfo(self: *ExtentStore, extent_id: u64) ?ExtentInfo {
        self.beginOp() catch return null;
        defer self.endOp();

        return self.lookupExtentInfo(extent_id);
    }

    /// Total number of extents (including deleted).
    pub fn extentCount(self: *ExtentStore) usize {
        self.beginOp() catch return 0;
        defer self.endOp();

        return self.extent_info_map.count();
    }

    /// Whether the store contains an extent with the given ID.
    pub fn hasExtent(self: *ExtentStore, extent_id: u64) bool {
        self.beginOp() catch return false;
        defer self.endOp();

        return self.extent_info_map.contains(extent_id);
    }

    /// Get a tiny extent ID from the available pool.
    pub fn getAvailableTinyExtent(self: *ExtentStore) ?u64 {
        return self.available_tiny.pop();
    }

    /// Return a tiny extent ID to the available pool.
    pub fn putAvailableTinyExtent(self: *ExtentStore, id: u64) bool {
        return self.available_tiny.push(id);
    }

    /// Total used size across all non-deleted extents.
    pub fn usedSize(self: *ExtentStore) u64 {
        self.beginOp() catch return 0;
        defer self.endOp();

        var total: u64 = 0;
        for (&self.extent_info_map.shards) |*s| {
            s.lock.lockShared();
            defer s.lock.unlockShared();

            var it = s.map.valueIterator();
            while (it.next()) |v| {
                if (!v.is_deleted) {
                    total += v.size;
                }
            }
        }
        return total;
    }

    /// Flush all cached extents to disk.
    pub fn flush(self: *ExtentStore) err.Error!void {
        try self.beginOp();
        defer self.endOp();

        self.extent_lifecycle_lock.lockShared();  // shared — flush is read-like
        defer self.extent_lifecycle_lock.unlockShared();

        try self.cache.flushAll();
    }

    /// Read data from an extent without CRC verification (for stream reads).
    pub fn readNoCrc(self: *ExtentStore, extent_id: u64, offset: u64, buf: []u8) err.Error!usize {
        try self.beginOp();
        defer self.endOp();

        self.extent_lifecycle_lock.lockShared();  // shared — read path
        defer self.extent_lifecycle_lock.unlockShared();

        if (self.closed.load(.acquire)) {
            return error.ExtentDeleted;
        }
        const ext = try self.getOrOpenExtent(extent_id);
        return try ext.read(offset, buf);
    }

    /// Get the current apply ID.
    pub fn applyId(self: *ExtentStore) u64 {
        return self.apply_id.load(.acquire);
    }

    /// Set the apply ID.
    pub fn setApplyId(self: *ExtentStore, id: u64) void {
        self.apply_id.store(id, .release);
    }

    /// Punch a hole in a tiny extent (for partial deletion).
    pub fn punchDelete(self: *ExtentStore, extent_id: u64, offset: u64, size: u64) err.Error!void {
        try self.beginOp();
        defer self.endOp();

        self.extent_lifecycle_lock.lock();  // exclusive — lifecycle mutation
        defer self.extent_lifecycle_lock.unlock();

        const ext = try self.getOrOpenExtent(extent_id);
        try ext.punchHole(offset, size);

        // Record the tiny extent deletion
        if (self.tiny_delete_recorder) |*tdr| {
            tdr.record(extent_id, offset, size) catch {};
        }
    }

    /// Lock a batch of extents for GC/backup.
    pub fn batchLockExtents(self: *ExtentStore, locks: []const ExtentLock) !void {
        self.lock_mu.lock();
        defer self.lock_mu.unlock();
        for (locks) |lk| {
            self.extent_lock_map.put(lk.extent_id, lk.flag) catch return error.IoError;
        }
        self.extent_lock_enabled.store(true, .release);
    }

    /// Unlock all extents (clear all GC locks).
    pub fn batchUnlockExtents(self: *ExtentStore) void {
        self.lock_mu.lock();
        defer self.lock_mu.unlock();
        self.extent_lock_map.clearRetainingCapacity();
        self.extent_lock_enabled.store(false, .release);
    }

    /// Check if an extent is GC-locked.
    pub fn getExtentLock(self: *ExtentStore, extent_id: u64) ?GcFlag {
        if (!self.extent_lock_enabled.load(.acquire)) return null;
        self.lock_mu.lock();
        defer self.lock_mu.unlock();
        return self.extent_lock_map.get(extent_id);
    }

    /// Persist the current apply ID to disk.
    pub fn persistApplyId(self: *ExtentStore) !void {
        try self.beginOp();
        defer self.endOp();

        apply_id_mod.persist(self.data_path, self.apply_id.load(.acquire)) catch {};
    }

    /// Read all tiny delete records.
    pub fn readTinyDeleteRecords(self: *ExtentStore, allocator: Allocator) ![]tiny_del_mod.TinyDeleteRecord {
        try self.beginOp();
        defer self.endOp();

        if (self.tiny_delete_recorder) |*tdr| {
            return tdr.readAll(allocator);
        }
        return &.{};
    }

    /// Close the store.
    pub fn close(self: *ExtentStore) void {
        self.closed.store(true, .release);

        // Wait for in-flight operations to drain.
        self.op_mu.lock();
        while (self.active_ops.load(.acquire) != 0) {
            self.op_cv.wait(&self.op_mu);
        }
        self.op_mu.unlock();

        self.extent_lifecycle_lock.lock();  // exclusive — closing
        defer self.extent_lifecycle_lock.unlock();

        self.cache.clear();
    }

    /// Generate the file path for an extent (thread-safe via threadlocal buffer).
    fn extentFilePath(self: *ExtentStore, extent_id: u64) [:0]const u8 {
        const S = struct {
            threadlocal var buf: [4096]u8 = undefined;
        };
        const written = std.fmt.bufPrintZ(&S.buf, "{s}/{d}", .{ self.data_path, extent_id }) catch {
            return self.data_path; // fallback
        };
        return written;
    }

    /// Get an extent from cache or open it from disk.
    /// Fast path (cache hit): no outer lock — cache.get() uses internal shared RwLock.
    /// Slow path (cache miss): acquires extent_open_mu to prevent duplicate opens,
    /// then double-checks cache before opening from disk.
    fn getOrOpenExtent(self: *ExtentStore, extent_id: u64) err.Error!*Extent {
        // Validate extent exists and is not deleted (shard map has per-shard RwLock).
        const info = self.lookupExtentInfo(extent_id) orelse return error.ExtentNotFound;
        if (info.is_deleted) {
            return error.ExtentDeleted;
        }

        // Fast path: cache hit with no outer lock.
        if (self.cache.get(extent_id)) |ext| {
            return ext;
        }

        // Slow path: cache miss — lock to prevent duplicate opens.
        self.extent_open_mu.lock();
        defer self.extent_open_mu.unlock();

        // Double-check cache after acquiring lock (another thread may have opened it).
        if (self.cache.get(extent_id)) |ext| {
            return ext;
        }

        // Re-check extent info (may have been deleted while we waited for the lock).
        const current_info = self.lookupExtentInfo(extent_id) orelse return error.ExtentNotFound;
        if (current_info.is_deleted) {
            return error.ExtentDeleted;
        }

        // Open from disk
        const path = self.extentFilePath(extent_id);

        // Check if file exists
        std.fs.cwd().access(path, .{}) catch return error.ExtentNotFound;

        const ext = try Extent.open(self.allocator, path, extent_id);

        // Final safety check after open (extent could be deleted during disk I/O).
        const final_info = self.lookupExtentInfo(extent_id) orelse {
            ext.close() catch {};
            ext.destroy();
            return error.ExtentNotFound;
        };
        if (final_info.is_deleted) {
            ext.close() catch {};
            ext.destroy();
            return error.ExtentDeleted;
        }

        self.cache.put(extent_id, ext);
        return ext;
    }

    /// Scan existing extent files on disk and populate metadata.
    fn loadExistingExtents(self: *ExtentStore) !void {
        var dir = std.fs.cwd().openDir(self.data_path, .{ .iterate = true }) catch return;
        defer dir.close();

        var max_id: u64 = constants.MIN_EXTENT_ID;

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind != .file) continue;

            // Parse extent ID from filename
            const extent_id = std.fmt.parseInt(u64, entry.name, 10) catch continue;

            // Get file size from stat
            const path = self.extentFilePath(extent_id);
            const stat = std.fs.cwd().statFile(path) catch continue;

            const modify_time: i64 = @intCast(@divFloor(stat.mtime, std.time.ns_per_s));

            const info = ExtentInfo{
                .file_id = extent_id,
                .size = stat.size,
                .modify_time = modify_time,
            };

            self.extent_info_map.put(extent_id, info);

            if (extent_id >= max_id) {
                max_id = extent_id + 1;
            }

            // Track available tiny extents
            if (extent_id > 0 and extent_id <= constants.TINY_EXTENT_COUNT) {
                _ = self.available_tiny.push(extent_id);
            }
        }

        self.base_extent_id.store(max_id, .seq_cst);
    }
};
