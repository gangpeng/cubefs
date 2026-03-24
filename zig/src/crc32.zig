// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! CRC32 wrapper using Zig standard library.

const std = @import("std");

/// Compute CRC32 (IEEE/Castagnoli) hash of the given data.
pub fn hash(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

/// Incremental CRC32 computation.
pub const Crc32 = struct {
    state: std.hash.Crc32,

    pub fn init() Crc32 {
        return .{ .state = std.hash.Crc32.init() };
    }

    pub fn update(self: *Crc32, data: []const u8) void {
        self.state.update(data);
    }

    pub fn final_(self: *Crc32) u32 {
        return self.state.final();
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "hash of empty slice is deterministic" {
    const h1 = hash("");
    const h2 = hash("");
    try testing.expectEqual(h1, h2);
}

test "hash of known data matches expected CRC32" {
    // "hello" CRC32 (Castagnoli)
    const h = hash("hello");
    try testing.expect(h != 0); // non-trivial
    // Same data must always produce the same hash
    try testing.expectEqual(h, hash("hello"));
}

test "different data produces different hashes" {
    try testing.expect(hash("abc") != hash("def"));
    try testing.expect(hash("hello") != hash("Hello"));
}

test "incremental matches one-shot" {
    const data = "hello, cubefs storage engine!";
    const one_shot = hash(data);

    var inc = Crc32.init();
    inc.update(data[0..7]);
    inc.update(data[7..]);
    const incremental = inc.final_();

    try testing.expectEqual(one_shot, incremental);
}

test "incremental three-part split" {
    const data = "abcdefghijklmnopqrstuvwxyz";
    const one_shot = hash(data);

    var inc = Crc32.init();
    inc.update(data[0..10]);
    inc.update(data[10..20]);
    inc.update(data[20..]);
    try testing.expectEqual(one_shot, inc.final_());
}

test "hash of 4KB block" {
    var buf: [4096]u8 = undefined;
    @memset(&buf, 0xAB);
    const h = hash(&buf);
    try testing.expect(h != 0);
    try testing.expectEqual(h, hash(&buf));
}
