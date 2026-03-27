// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Extent cache with O(1) amortized eviction via Clock (second-chance) algorithm.
//!
//! Two maps: `normal` (evictable) and `tiny` (pinned, never evicted).
//! Normal entries are heap-allocated CacheEntry nodes in a doubly-linked ring.
//!
//! Key design: `get()` uses a **shared** RwLock — concurrent reads never block
//! each other. It marks the entry as "accessed" via an atomic bool instead of
//! moving it in the linked list. Eviction (Clock sweep) runs under an exclusive
//! lock: walk from head; if accessed, clear the flag and advance (second chance);
//! if not accessed, evict. This is O(1) amortized for steady-state workloads.

const std = @import("std");
const Allocator = std.mem.Allocator;
const RwLock = std.Thread.RwLock;
const constants = @import("constants.zig");
const err = @import("error.zig");
const extent_mod = @import("extent.zig");
const Extent = extent_mod.Extent;

const CacheEntry = struct {
    extent: *Extent,
    extent_id: u64,
    /// Set atomically on access; cleared by eviction sweep (second chance).
    accessed: std.atomic.Value(bool),
    prev: ?*CacheEntry = null,
    next: ?*CacheEntry = null,

    fn init(ext: *Extent, id: u64) CacheEntry {
        return .{
            .extent = ext,
            .extent_id = id,
            .accessed = std.atomic.Value(bool).init(true),
        };
    }
};

pub const ExtentCache = struct {
    /// Normal extent cache (evictable). Maps extent_id → *CacheEntry (heap-allocated).
    normal: std.AutoHashMap(u64, *CacheEntry),
    /// RwLock: get() takes shared, put/remove/evict/clear take exclusive.
    normal_lock: RwLock,
    /// Clock hand for eviction sweep (points into the linked ring).
    clock_hand: ?*CacheEntry,
    /// LRU list head (for traversal / clear).
    lru_head: ?*CacheEntry,
    /// LRU list tail (for append).
    lru_tail: ?*CacheEntry,
    /// Tiny extent cache (never evicted).
    tiny: std.AutoHashMap(u64, *Extent),
    tiny_lock: RwLock,
    /// Maximum number of normal extents to cache.
    capacity: usize,
    allocator: Allocator,

    pub fn init(allocator: Allocator, capacity: usize) ExtentCache {
        return ExtentCache{
            .normal = std.AutoHashMap(u64, *CacheEntry).init(allocator),
            .normal_lock = RwLock{},
            .clock_hand = null,
            .lru_head = null,
            .lru_tail = null,
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
    /// Uses shared lock — concurrent gets never block each other.
    /// Marks the entry as accessed (atomic) for Clock eviction.
    pub fn get(self: *ExtentCache, extent_id: u64) ?*Extent {
        if (isTiny(extent_id)) {
            self.tiny_lock.lockShared();
            defer self.tiny_lock.unlockShared();
            return self.tiny.get(extent_id);
        }

        self.normal_lock.lockShared();
        defer self.normal_lock.unlockShared();

        if (self.normal.get(extent_id)) |entry| {
            // Mark as recently accessed — atomic, no list manipulation needed
            entry.accessed.store(true, .release);
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

        // Check if already present — update extent, mark accessed
        if (self.normal.get(extent_id)) |existing| {
            existing.extent = ext;
            existing.accessed.store(true, .release);
            return;
        }

        // Evict if at capacity
        if (self.normal.count() >= self.capacity) {
            self.evictOneLocked();
        }

        // Allocate new entry
        const entry = self.allocator.create(CacheEntry) catch return;
        entry.* = CacheEntry.init(ext, extent_id);

        self.normal.put(extent_id, entry) catch {
            self.allocator.destroy(entry);
            return;
        };

        self.appendTailLocked(entry);
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
        if (result) |kv| {
            const entry = kv.value;
            // If clock_hand points at this entry, advance it first
            if (self.clock_hand == entry) {
                self.clock_hand = entry.next;
            }
            self.unlinkLocked(entry);
            const ext = entry.extent;
            self.allocator.destroy(entry);
            return ext;
        }
        return null;
    }

    /// Clock (second-chance) eviction — O(1) amortized.
    /// Walk from clock_hand: if accessed, give a second chance (clear flag, advance);
    /// if not accessed, evict. Must be called with exclusive lock held.
    fn evictOneLocked(self: *ExtentCache) void {
        const count = self.normal.count();
        if (count == 0) return;

        // Initialize clock hand if needed
        if (self.clock_hand == null) {
            self.clock_hand = self.lru_head;
        }

        // Sweep at most 2*count entries (guarantees finding a victim)
        var remaining: usize = count * 2;
        while (remaining > 0) : (remaining -= 1) {
            const hand = self.clock_hand orelse self.lru_head orelse return;

            if (hand.accessed.load(.acquire)) {
                // Second chance: clear accessed flag, advance hand
                hand.accessed.store(false, .release);
                self.clock_hand = hand.next orelse self.lru_head;
            } else {
                // Victim found — advance hand past this entry first
                self.clock_hand = hand.next orelse self.lru_head;
                if (self.clock_hand == hand) {
                    self.clock_hand = null; // was the only entry
                }

                const extent_id = hand.extent_id;
                _ = self.normal.fetchRemove(extent_id);
                self.unlinkLocked(hand);

                hand.extent.close() catch {};
                hand.extent.destroy();
                self.allocator.destroy(hand);
                return;
            }
        }

        // Fallback: all entries were recently accessed; evict head
        const head = self.lru_head orelse return;
        self.clock_hand = head.next orelse null;

        const extent_id = head.extent_id;
        _ = self.normal.fetchRemove(extent_id);
        self.unlinkLocked(head);

        head.extent.close() catch {};
        head.extent.destroy();
        self.allocator.destroy(head);
    }

    /// Close and remove all cached extents.
    pub fn clear(self: *ExtentCache) void {
        {
            self.normal_lock.lock();
            defer self.normal_lock.unlock();

            var node = self.lru_head;
            while (node) |n| {
                const next = n.next;
                n.extent.close() catch {};
                n.extent.destroy();
                self.allocator.destroy(n);
                node = next;
            }
            self.lru_head = null;
            self.lru_tail = null;
            self.clock_hand = null;
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

            var node = self.lru_head;
            while (node) |n| {
                try n.extent.flush();
                node = n.next;
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

    // ── Intrusive linked list helpers (must be called with exclusive lock held) ──

    /// Remove entry from the doubly-linked list.
    fn unlinkLocked(self: *ExtentCache, entry: *CacheEntry) void {
        if (entry.prev) |prev| {
            prev.next = entry.next;
        } else {
            self.lru_head = entry.next;
        }
        if (entry.next) |next| {
            next.prev = entry.prev;
        } else {
            self.lru_tail = entry.prev;
        }
        entry.prev = null;
        entry.next = null;
    }

    /// Append entry at the tail.
    fn appendTailLocked(self: *ExtentCache, entry: *CacheEntry) void {
        entry.prev = self.lru_tail;
        entry.next = null;
        if (self.lru_tail) |tail| {
            tail.next = entry;
        } else {
            self.lru_head = entry;
        }
        self.lru_tail = entry;
    }
};
