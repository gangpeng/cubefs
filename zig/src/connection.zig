// connection.zig — Per-connection packet loop with timeouts and metrics.

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const net = std.net;
const codec = @import("codec.zig");
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const handler_mod = @import("handler.zig");
const log = @import("log.zig");
const Packet = pkt_mod.Packet;
const ServerInner = handler_mod.ServerInner;

const READ_TIMEOUT_SECS: u32 = 60;
const WRITE_TIMEOUT_SECS: u32 = 30;

// Module-level atomic counters for connection metrics.
var active_connections: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);
var total_requests: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Connection metrics exported for admin /stats endpoint.
pub const Metrics = struct {
    active_connections: u64,
    total_requests: u64,
};

/// Get current connection metrics.
pub fn getMetrics() Metrics {
    return .{
        .active_connections = active_connections.load(.acquire),
        .total_requests = total_requests.load(.acquire),
    };
}

/// Set SO_RCVTIMEO on a socket.
fn setReadTimeout(stream: net.Stream, timeout_secs: u32) void {
    const timeval = posix.system.timeval{
        .sec = @intCast(timeout_secs),
        .usec = 0,
    };
    const rc = posix.system.setsockopt(
        stream.handle,
        posix.system.SOL.SOCKET,
        posix.system.SO.RCVTIMEO,
        std.mem.asBytes(&timeval),
        @sizeOf(posix.system.timeval),
    );
    if (rc != 0) {
        log.debug("failed to set SO_RCVTIMEO", .{});
    }
}

/// Set SO_SNDTIMEO on a socket.
fn setWriteTimeout(stream: net.Stream, timeout_secs: u32) void {
    const timeval = posix.system.timeval{
        .sec = @intCast(timeout_secs),
        .usec = 0,
    };
    const rc = posix.system.setsockopt(
        stream.handle,
        posix.system.SOL.SOCKET,
        posix.system.SO.SNDTIMEO,
        std.mem.asBytes(&timeval),
        @sizeOf(posix.system.timeval),
    );
    if (rc != 0) {
        log.debug("failed to set SO_SNDTIMEO", .{});
    }
}

/// Reset connection metrics (for testing).
pub fn resetMetrics() void {
    active_connections.store(0, .release);
    total_requests.store(0, .release);
}

/// Handle one TCP connection: read packets, dispatch, write responses.
pub fn handleConnection(stream: net.Stream, server: *ServerInner, allocator: Allocator) void {
    handleConnectionTimeout(stream, server, allocator, READ_TIMEOUT_SECS);
}

/// Handle one TCP connection with a custom read timeout (in seconds).
pub fn handleConnectionTimeout(stream: net.Stream, server: *ServerInner, allocator: Allocator, read_timeout: u32) void {
    defer stream.close();

    // Set socket timeouts
    setReadTimeout(stream, read_timeout);
    setWriteTimeout(stream, WRITE_TIMEOUT_SECS);

    // Track active connection
    _ = active_connections.fetchAdd(1, .release);
    defer _ = active_connections.fetchSub(1, .release);

    const reader = stream.reader();
    const writer = stream.writer();

    while (true) {
        // Read one packet
        var pkt = codec.readPacket(reader, allocator) catch |e| {
            switch (e) {
                error.ConnectionClosed => {},
                error.InvalidMagic => log.warn("invalid magic byte, closing connection", .{}),
                error.PayloadTooLarge => log.warn("payload too large, closing connection", .{}),
                else => log.warn("read error: {}, closing connection", .{e}),
            }
            return;
        };
        defer pkt.deinit();

        // Track request count
        _ = total_requests.fetchAdd(1, .release);

        // Dispatch
        var reply = server.dispatch(&pkt);
        defer reply.deinit();

        // Write response
        codec.writePacket(writer, &reply) catch |e| {
            switch (e) {
                error.ConnectionClosed => {},
                else => log.warn("write error: {}, closing connection", .{e}),
            }
            return;
        };
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "getMetrics returns zeroes after reset" {
    resetMetrics();
    const m = getMetrics();
    try testing.expectEqual(@as(u64, 0), m.active_connections);
    try testing.expectEqual(@as(u64, 0), m.total_requests);
}

test "Metrics struct fields" {
    const m = Metrics{
        .active_connections = 10,
        .total_requests = 42,
    };
    try testing.expectEqual(@as(u64, 10), m.active_connections);
    try testing.expectEqual(@as(u64, 42), m.total_requests);
}
