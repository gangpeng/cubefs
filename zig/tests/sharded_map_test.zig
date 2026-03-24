// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

const std = @import("std");
const testing = std.testing;
const engine = @import("cubefs_engine");
const sharded_map_mod = engine._sharded_map;

test "basic put and get" {
    const Map = sharded_map_mod.ShardedMap(u64, u32);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    map.put(1, 100);
    map.put(2, 200);
    map.put(3, 300);

    try testing.expectEqual(@as(?u32, 100), map.get(1));
    try testing.expectEqual(@as(?u32, 200), map.get(2));
    try testing.expectEqual(@as(?u32, 300), map.get(3));
    try testing.expectEqual(@as(?u32, null), map.get(4));
}

test "remove" {
    const Map = sharded_map_mod.ShardedMap(u64, u32);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    map.put(10, 42);
    try testing.expect(map.contains(10));

    const old = map.remove(10);
    try testing.expectEqual(@as(?u32, 42), old);
    try testing.expect(!map.contains(10));

    try testing.expectEqual(@as(?u32, null), map.remove(10));
}

test "count" {
    const Map = sharded_map_mod.ShardedMap(u64, u32);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    try testing.expectEqual(@as(usize, 0), map.count());

    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        map.put(i, @intCast(i));
    }

    try testing.expectEqual(@as(usize, 100), map.count());
}

test "overwrite value" {
    const Map = sharded_map_mod.ShardedMap(u64, u32);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    map.put(1, 100);
    try testing.expectEqual(@as(?u32, 100), map.get(1));

    map.put(1, 200);
    try testing.expectEqual(@as(?u32, 200), map.get(1));
    try testing.expectEqual(@as(usize, 1), map.count());
}

test "contains" {
    const Map = sharded_map_mod.ShardedMap(u64, u32);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    try testing.expect(!map.contains(42));
    map.put(42, 1);
    try testing.expect(map.contains(42));
}

test "many keys across shards" {
    const Map = sharded_map_mod.ShardedMap(u64, u64);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        map.put(i, i * 10);
    }

    try testing.expectEqual(@as(usize, 1000), map.count());

    i = 0;
    while (i < 1000) : (i += 1) {
        const val = map.get(i);
        try testing.expect(val != null);
        try testing.expectEqual(i * 10, val.?);
    }
}

const ConcurrentState = struct {
    map: *sharded_map_mod.ShardedMap(u64, u64),
    start: u64,
    count: u64,
};

const HotKeyState = struct {
    map: *sharded_map_mod.ShardedMap(u64, u64),
    key: u64,
    count: u64,
};

fn concurrentPutWorker(state: *const ConcurrentState) void {
    var i: u64 = 0;
    while (i < state.count) : (i += 1) {
        const key = state.start + i;
        state.map.put(key, key * 2);
    }
}

fn incrementValue(value: *u64) void {
    value.* += 1;
}

fn concurrentIncrementWorker(state: *const HotKeyState) void {
    var i: u64 = 0;
    while (i < state.count) : (i += 1) {
        const updated = state.map.getMut(state.key, incrementValue);
        std.debug.assert(updated);
    }
}

test "concurrent put/get across threads" {
    const Map = sharded_map_mod.ShardedMap(u64, u64);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    const per_thread: u64 = 2000;
    var states = [_]ConcurrentState{
        .{ .map = &map, .start = 0, .count = per_thread },
        .{ .map = &map, .start = per_thread, .count = per_thread },
        .{ .map = &map, .start = per_thread * 2, .count = per_thread },
        .{ .map = &map, .start = per_thread * 3, .count = per_thread },
    };

    var threads: [states.len]std.Thread = undefined;
    for (&threads, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, concurrentPutWorker, .{&states[i]});
    }
    for (threads) |t| t.join();

    const total = per_thread * states.len;
    try testing.expectEqual(@as(usize, @intCast(total)), map.count());

    var key: u64 = 0;
    while (key < total) : (key += 1) {
        try testing.expectEqual(@as(?u64, key * 2), map.get(key));
    }
}

test "concurrent getMut increments on one key" {
    const Map = sharded_map_mod.ShardedMap(u64, u64);
    var map = Map.init(testing.allocator);
    defer map.deinit();

    const hot_key: u64 = 42;
    const per_thread: u64 = 5000;
    map.put(hot_key, 0);

    var states = [_]HotKeyState{
        .{ .map = &map, .key = hot_key, .count = per_thread },
        .{ .map = &map, .key = hot_key, .count = per_thread },
        .{ .map = &map, .key = hot_key, .count = per_thread },
        .{ .map = &map, .key = hot_key, .count = per_thread },
    };

    var threads: [states.len]std.Thread = undefined;
    for (&threads, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, concurrentIncrementWorker, .{&states[i]});
    }
    for (threads) |t| t.join();

    try testing.expectEqual(@as(usize, 1), map.count());
    try testing.expectEqual(@as(?u64, per_thread * states.len), map.get(hot_key));
}
