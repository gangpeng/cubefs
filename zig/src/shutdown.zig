// shutdown.zig — Graceful shutdown and signal handling.
// Catches SIGTERM/SIGINT and performs ordered shutdown:
// 1. Stop accepting new connections
// 2. Stop repair manager
// 3. Persist partition metadata and apply IDs
// 4. Stop raft server
// 5. Close connection pools

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const log = @import("log.zig");

/// Shutdown state — shared between signal handler and shutdown coordinator.
var shutdown_requested = std.atomic.Value(bool).init(false);

/// Check whether a shutdown has been requested.
pub fn isShutdownRequested() bool {
    return shutdown_requested.load(.acquire);
}

/// Request a shutdown (can be called from any context).
pub fn requestShutdown() void {
    shutdown_requested.store(true, .release);
}

/// Component that can be stopped during shutdown.
pub const ShutdownComponent = struct {
    name: []const u8,
    stop_fn: *const fn (ctx: *anyopaque) void,
    ctx: *anyopaque,
};

/// Manages ordered shutdown of registered components.
pub const ShutdownCoordinator = struct {
    components: std.ArrayList(ShutdownComponent),
    allocator: Allocator,

    pub fn init(allocator: Allocator) ShutdownCoordinator {
        return .{
            .components = std.ArrayList(ShutdownComponent).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ShutdownCoordinator) void {
        self.components.deinit();
    }

    /// Register a component to be stopped during shutdown.
    /// Components are stopped in reverse registration order (LIFO).
    pub fn register(self: *ShutdownCoordinator, component: ShutdownComponent) void {
        self.components.append(component) catch {};
    }

    /// Execute ordered shutdown of all registered components.
    pub fn executeShutdown(self: *ShutdownCoordinator) void {
        log.info("shutdown: starting ordered shutdown ({d} components)", .{self.components.items.len});

        // Stop in reverse order (LIFO)
        var i = self.components.items.len;
        while (i > 0) {
            i -= 1;
            const comp = self.components.items[i];
            log.info("shutdown: stopping {s}...", .{comp.name});
            comp.stop_fn(comp.ctx);
        }

        log.info("shutdown: all components stopped", .{});
    }
};

/// Install signal handlers for SIGTERM and SIGINT.
/// On signal, sets the shutdown flag and optionally calls a callback.
pub fn installSignalHandlers() void {
    const handler = posix.Sigaction{
        .handler = .{ .handler = signalHandler },
        .mask = posix.empty_sigset,
        .flags = 0,
    };

    posix.sigaction(posix.SIG.TERM, &handler, null);
    posix.sigaction(posix.SIG.INT, &handler, null);
}

fn signalHandler(sig: c_int) callconv(.C) void {
    _ = sig;
    shutdown_requested.store(true, .release);
}

/// Wait for a shutdown signal, then execute the shutdown sequence.
/// Blocks until SIGTERM/SIGINT is received.
pub fn waitForShutdown(coordinator: *ShutdownCoordinator) void {
    while (!shutdown_requested.load(.acquire)) {
        std.time.sleep(100 * std.time.ns_per_ms);
    }
    coordinator.executeShutdown();
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "shutdown coordinator executes in reverse order" {
    var order_buf: [10]usize = undefined;
    var order_idx = std.atomic.Value(usize).init(0);

    const StopCtx = struct {
        idx: usize,
        order_buf: *[10]usize,
        order_idx: *std.atomic.Value(usize),

        fn stopFn(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const pos = self.order_idx.fetchAdd(1, .seq_cst);
            self.order_buf[pos] = self.idx;
        }
    };

    var ctx0 = StopCtx{ .idx = 0, .order_buf = &order_buf, .order_idx = &order_idx };
    var ctx1 = StopCtx{ .idx = 1, .order_buf = &order_buf, .order_idx = &order_idx };
    var ctx2 = StopCtx{ .idx = 2, .order_buf = &order_buf, .order_idx = &order_idx };

    var coordinator = ShutdownCoordinator.init(testing.allocator);
    defer coordinator.deinit();

    coordinator.register(.{ .name = "first", .stop_fn = StopCtx.stopFn, .ctx = @ptrCast(&ctx0) });
    coordinator.register(.{ .name = "second", .stop_fn = StopCtx.stopFn, .ctx = @ptrCast(&ctx1) });
    coordinator.register(.{ .name = "third", .stop_fn = StopCtx.stopFn, .ctx = @ptrCast(&ctx2) });

    coordinator.executeShutdown();

    // Should be stopped in reverse: third(2), second(1), first(0)
    try testing.expectEqual(@as(usize, 2), order_buf[0]);
    try testing.expectEqual(@as(usize, 1), order_buf[1]);
    try testing.expectEqual(@as(usize, 0), order_buf[2]);
}

test "shutdown flag defaults to false" {
    // Reset for test (note: global state)
    shutdown_requested.store(false, .release);
    try testing.expect(!isShutdownRequested());
}

test "requestShutdown sets flag" {
    shutdown_requested.store(false, .release);
    requestShutdown();
    try testing.expect(isShutdownRequested());
    // Reset
    shutdown_requested.store(false, .release);
}
