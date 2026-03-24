// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Extent cache with approximate LRU eviction.
//!
//! Two maps: `normal` (evictable) and `tiny` (pinned, never evicted).
//! Uses a global atomic counter for approximate LRU ordering.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Mutex = std.Thread.Mutex;
const RwLock = std.Thread.RwLock;
const constants = @import("constants.zig");
const err = @import("error.zig");
const extent_mod = @import("extent.zig");
const Extent = extent_mod.Extent;

/// Global monotonic counter for LRU ordering.
var access_counter: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

const CacheEntry = struct {
    extent: *Extent,
    access_order: std.atomic.Value(u64),

    fn init(ext: *Extent) CacheEntry {
        return .{
            .extent = ext,
            .access_order = std.atomic.Value(u64).init(access_counter.fetchAdd(1, .monotonic)),
        };
    }

    fn touch(self: *CacheEntry) void {
        // Coarse-grained LRU: only update order every 64 accesses to reduce
        // contention on the global atomic counter.
        const current = self.access_order.load(.monotonic);
        const global = access_counter.load(.monotonic);
        if (global -% current > 64) {
            self.access_order.store(access_counter.fetchAdd(1, .monotonic), .monotonic);
        }
    }
};

pub const ExtentCache = struct {
    /// Normal extent cache (evictable).
    normal: std.AutoHashMap(u64, CacheEntry),
    normal_lock: RwLock,
    /// Tiny extent cache (never evicted).
    tiny: std.AutoHashMap(u64, *Extent),
    tiny_lock: RwLock,
    /// Maximum number of normal extents to cache.
    capacity: usize,
    allocator: Allocator,

    pub fn init(allocator: Allocator, capacity: usize) ExtentCache {
        return ExtentCache{
            .normal = std.AutoHashMap(u64, CacheEntry).init(allocator),
            .normal_lock = RwLock{},
            .tiny = std.AutoHashMap(u64, *Extent).init(allocator),
            .tiny_lock = RwLock{},
            .capacity = capacity,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ExtentCache) void {
        self.normal.deinit();
        self.tiny.deinit();
    }

    fn isTiny(extent_id: u64) bool {
        return extent_id > 0 and extent_id <= constants.TINY_EXTENT_COUNT;
    }

    /// Get a cached extent by ID.
    pub fn get(self: *ExtentCache, extent_id: u64) ?*Extent {
        if (isTiny(extent_id)) {
            self.tiny_lock.lockShared();
            defer self.tiny_lock.unlockShared();
            return self.tiny.get(extent_id);
        }

        self.normal_lock.lockShared();
        defer self.normal_lock.unlockShared();

        if (self.normal.getPtr(extent_id)) |entry| {
            entry.touch();
            return entry.extent;
        }
        return null;
    }

    /// Insert an extent into the cache.
    pub fn put(self: *ExtentCache, extent_id: u64, ext: *Extent) void {
        if (isTiny(extent_id)) {
            self.tiny_lock.lock();
            defer self.tiny_lock.unlock();
            self.tiny.put(extent_id, ext) catch {};
            return;
        }

        self.normal_lock.lock();
        defer self.normal_lock.unlock();

        // Evict if at capacity
        if (self.normal.count() >= self.capacity and !self.normal.contains(extent_id)) {
            self.evictOneLocked();
        }

        self.normal.put(extent_id, CacheEntry.init(ext)) catch {};
    }

    /// Remove an extent from the cache. Returns the extent if found.
    pub fn remove(self: *ExtentCache, extent_id: u64) ?*Extent {
        if (isTiny(extent_id)) {
            self.tiny_lock.lock();
            defer self.tiny_lock.unlock();
            const result = self.tiny.fetchRemove(extent_id);
            if (result) |kv| return kv.value;
            return null;
        }

        self.normal_lock.lock();
        defer self.normal_lock.unlock();

        const result = self.normal.fetchRemove(extent_id);
        if (result) |kv| return kv.value.extent;
        return null;
    }

    /// Evict the least recently accessed entry (must be called with lock held).
    fn evictOneLocked(self: *ExtentCache) void {
        var oldest_id: u64 = 0;
        var oldest_order: u64 = std.math.maxInt(u64);

        var it = self.normal.iterator();
        while (it.next()) |entry| {
            const t = entry.value_ptr.access_order.load(.monotonic);
            if (t < oldest_order) {
                oldest_order = t;
                oldest_id = entry.key_ptr.*;
            }
        }

        if (oldest_id != 0) {
            if (self.normal.fetchRemove(oldest_id)) |kv| {
                kv.value.extent.*.close() catch {};
                kv.value.extent.*.destroy();
            }
        }
    }

    /// Close and remove all cached extents.
    pub fn clear(self: *ExtentCache) void {
        {
            self.normal_lock.lock();
            defer self.normal_lock.unlock();

            var it = self.normal.valueIterator();
            while (it.next()) |entry| {
                entry.extent.*.close() catch {};
                entry.extent.*.destroy();
            }
            self.normal.clearAndFree();
        }

        {
            self.tiny_lock.lock();
            defer self.tiny_lock.unlock();

            var it = self.tiny.valueIterator();
            while (it.next()) |ext| {
                ext.*.close() catch {};
                ext.*.destroy();
            }
            self.tiny.clearAndFree();
        }
    }

    /// Flush all cached extents to disk without closing them.
    pub fn flushAll(self: *ExtentCache) err.Error!void {
        {
            self.normal_lock.lockShared();
            defer self.normal_lock.unlockShared();

            var it = self.normal.valueIterator();
            while (it.next()) |entry| {
                try entry.extent.*.flush();
            }
        }

        {
            self.tiny_lock.lockShared();
            defer self.tiny_lock.unlockShared();

            var it = self.tiny.valueIterator();
            while (it.next()) |ext| {
                try ext.*.flush();
            }
        }
    }

    /// Number of cached normal extents.
    pub fn normalCount(self: *ExtentCache) usize {
        self.normal_lock.lockShared();
        defer self.normal_lock.unlockShared();
        return self.normal.count();
    }

    /// Number of cached tiny extents.
    pub fn tinyCount(self: *ExtentCache) usize {
        self.tiny_lock.lockShared();
        defer self.tiny_lock.unlockShared();
        return self.tiny.count();
    }
};
