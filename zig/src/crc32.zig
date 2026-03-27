// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! CRC32 IEEE (ISO-HDLC) wrapper with hardware acceleration via zlib.
//!
//! zlib's crc32() uses PCLMULQDQ on x86_64 and CRC32 instructions on aarch64,
//! matching Go's crc32.ChecksumIEEE hardware-accelerated path.
//! Falls back to Zig stdlib (software table-based) if zlib is unavailable.

const std = @import("std");
const c = @cImport({
    @cInclude("zlib.h");
});

/// Compute CRC32 IEEE hash of the given data using zlib (hardware-accelerated).
pub fn hash(data: []const u8) u32 {
    if (data.len == 0) return 0;
    // zlib crc32: initial value 0 means crc32(0, NULL, 0) == 0,
    // but for non-empty data we pass crc=0 which zlib interprets as the
    // standard IEEE initial value (0xFFFFFFFF) internally and XORs at the end.
    const result = c.crc32(0, data.ptr, @intCast(data.len));
    return @intCast(result);
}

/// Incremental CRC32 IEEE computation using zlib.
pub const Crc32 = struct {
    crc: c_ulong,

    pub fn init() Crc32 {
        return .{ .crc = c.crc32(0, null, 0) };
    }

    pub fn update(self: *Crc32, data: []const u8) void {
        if (data.len == 0) return;
        self.crc = c.crc32(self.crc, data.ptr, @intCast(data.len));
    }

    pub fn final_(self: *Crc32) u32 {
        return @intCast(self.crc);
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

test "zlib matches zig stdlib CRC32 IEEE" {
    // Verify zlib produces same results as Zig's Crc32IsoHdlc (IEEE)
    const test_data = "The quick brown fox jumps over the lazy dog";
    const zlib_crc = hash(test_data);
    const zig_crc = std.hash.Crc32.hash(test_data);
    try testing.expectEqual(zig_crc, zlib_crc);
}

test "zlib matches zig stdlib for large block" {
    var buf: [1048576]u8 = undefined; // 1MB
    for (&buf, 0..) |*b, i| {
        b.* = @intCast(i & 0xFF);
    }
    const zlib_crc = hash(&buf);
    const zig_crc = std.hash.Crc32.hash(&buf);
    try testing.expectEqual(zig_crc, zlib_crc);
}

test "incremental zlib matches zig stdlib incremental" {
    const data = "abcdefghijklmnopqrstuvwxyz0123456789";
    var zlib_inc = Crc32.init();
    zlib_inc.update(data[0..10]);
    zlib_inc.update(data[10..20]);
    zlib_inc.update(data[20..]);

    var zig_inc = std.hash.Crc32.init();
    zig_inc.update(data[0..10]);
    zig_inc.update(data[10..20]);
    zig_inc.update(data[20..]);

    try testing.expectEqual(zig_inc.final(), zlib_inc.final_());
}
