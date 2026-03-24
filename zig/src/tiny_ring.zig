// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Ring buffer for tiny extent ID allocation.
//!
//! Replaces Go's `chan uint64` based tiny extent allocation with a
//! mutex-protected ring buffer.

const std = @import("std");
const Mutex = std.Thread.Mutex;
const Allocator = std.mem.Allocator;

pub const TinyRing = struct {
    data: []u64,
    head: usize,
    tail: usize,
    count: usize,
    capacity: usize,
    mutex: Mutex,
    allocator: Allocator,

    pub fn init(allocator: Allocator, capacity: usize) !TinyRing {
        const data = try allocator.alloc(u64, capacity);
        @memset(data, 0);
        return TinyRing{
            .data = data,
            .head = 0,
            .tail = 0,
            .count = 0,
            .capacity = capacity,
            .mutex = Mutex{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TinyRing) void {
        self.allocator.free(self.data);
    }

    /// Push an extent ID into the ring. Returns false if full.
    pub fn push(self: *TinyRing, extent_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.count >= self.capacity) return false;

        self.data[self.tail] = extent_id;
        self.tail = (self.tail + 1) % self.capacity;
        self.count += 1;
        return true;
    }

    /// Pop the next available extent ID. Returns null if empty.
    pub fn pop(self: *TinyRing) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.count == 0) return null;

        const id = self.data[self.head];
        self.head = (self.head + 1) % self.capacity;
        self.count -= 1;
        return id;
    }

    /// Number of available extent IDs in the ring.
    pub fn len(self: *TinyRing) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.count;
    }

    /// Whether the ring is empty.
    pub fn isEmpty(self: *TinyRing) bool {
        return self.len() == 0;
    }

    /// Whether the ring is full.
    pub fn isFull(self: *TinyRing) bool {
        return self.len() >= self.capacity;
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "push and pop FIFO order" {
    var ring = try TinyRing.init(testing.allocator, 4);
    defer ring.deinit();

    try testing.expect(ring.push(10));
    try testing.expect(ring.push(20));
    try testing.expect(ring.push(30));

    try testing.expectEqual(@as(?u64, 10), ring.pop());
    try testing.expectEqual(@as(?u64, 20), ring.pop());
    try testing.expectEqual(@as(?u64, 30), ring.pop());
    try testing.expectEqual(@as(?u64, null), ring.pop());
}

test "pop from empty returns null" {
    var ring = try TinyRing.init(testing.allocator, 8);
    defer ring.deinit();

    try testing.expectEqual(@as(?u64, null), ring.pop());
    try testing.expect(ring.isEmpty());
}

test "push to full returns false" {
    var ring = try TinyRing.init(testing.allocator, 2);
    defer ring.deinit();

    try testing.expect(ring.push(1));
    try testing.expect(ring.push(2));
    try testing.expect(!ring.push(3)); // full
    try testing.expect(ring.isFull());
}

test "len tracks count" {
    var ring = try TinyRing.init(testing.allocator, 8);
    defer ring.deinit();

    try testing.expectEqual(@as(usize, 0), ring.len());
    _ = ring.push(1);
    try testing.expectEqual(@as(usize, 1), ring.len());
    _ = ring.push(2);
    try testing.expectEqual(@as(usize, 2), ring.len());
    _ = ring.pop();
    try testing.expectEqual(@as(usize, 1), ring.len());
}

test "wrap around" {
    var ring = try TinyRing.init(testing.allocator, 3);
    defer ring.deinit();

    // Fill it up
    _ = ring.push(1);
    _ = ring.push(2);
    _ = ring.push(3);

    // Drain two
    try testing.expectEqual(@as(?u64, 1), ring.pop());
    try testing.expectEqual(@as(?u64, 2), ring.pop());

    // Push two more (wraps around)
    try testing.expect(ring.push(4));
    try testing.expect(ring.push(5));

    // Pop remaining in order
    try testing.expectEqual(@as(?u64, 3), ring.pop());
    try testing.expectEqual(@as(?u64, 4), ring.pop());
    try testing.expectEqual(@as(?u64, 5), ring.pop());
    try testing.expectEqual(@as(?u64, null), ring.pop());
}

test "isEmpty and isFull" {
    var ring = try TinyRing.init(testing.allocator, 1);
    defer ring.deinit();

    try testing.expect(ring.isEmpty());
    try testing.expect(!ring.isFull());

    _ = ring.push(42);

    try testing.expect(!ring.isEmpty());
    try testing.expect(ring.isFull());

    _ = ring.pop();
    try testing.expect(ring.isEmpty());
}
