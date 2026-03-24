// log.zig — Logging wrapper around std.log with configurable level.

const std = @import("std");

pub const Level = enum {
    debug,
    info,
    warn,
    err,
};

var global_level: Level = .info;

pub fn setLevel(level: Level) void {
    global_level = level;
}

pub fn getLevel() Level {
    return global_level;
}

pub fn debug(comptime fmt: []const u8, args: anytype) void {
    if (@intFromEnum(global_level) <= @intFromEnum(Level.debug)) {
        std.log.debug(fmt, args);
    }
}

pub fn info(comptime fmt: []const u8, args: anytype) void {
    if (@intFromEnum(global_level) <= @intFromEnum(Level.info)) {
        std.log.info(fmt, args);
    }
}

pub fn warn(comptime fmt: []const u8, args: anytype) void {
    if (@intFromEnum(global_level) <= @intFromEnum(Level.warn)) {
        std.log.warn(fmt, args);
    }
}

pub fn err(comptime fmt: []const u8, args: anytype) void {
    std.log.err(fmt, args);
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "log level defaults to info" {
    try testing.expectEqual(Level.info, getLevel());
}

test "setLevel changes log level" {
    const original = getLevel();
    defer setLevel(original);

    setLevel(.debug);
    try testing.expectEqual(Level.debug, getLevel());

    setLevel(.warn);
    try testing.expectEqual(Level.warn, getLevel());

    setLevel(.err);
    try testing.expectEqual(Level.err, getLevel());
}

test "log level enum ordering" {
    try testing.expect(@intFromEnum(Level.debug) < @intFromEnum(Level.info));
    try testing.expect(@intFromEnum(Level.info) < @intFromEnum(Level.warn));
    try testing.expect(@intFromEnum(Level.warn) < @intFromEnum(Level.err));
}

test "debug not called at info level" {
    const original = getLevel();
    defer setLevel(original);

    setLevel(.info);
    // debug is suppressed at info level — just verify level gating logic
    try testing.expect(@intFromEnum(Level.debug) < @intFromEnum(getLevel()));
}

test "info suppressed at warn level" {
    const original = getLevel();
    defer setLevel(original);

    setLevel(.warn);
    // info is suppressed at warn level
    try testing.expect(@intFromEnum(Level.info) < @intFromEnum(getLevel()));
}

test "warn suppressed at err level" {
    const original = getLevel();
    defer setLevel(original);

    setLevel(.err);
    // warn is suppressed at err level
    try testing.expect(@intFromEnum(Level.warn) < @intFromEnum(getLevel()));
}
