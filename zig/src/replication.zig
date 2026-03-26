// replication.zig — Parallel forward + local exec for replicated writes.
// Uses a bounded work queue with a thread pool instead of per-request thread spawning.

const std = @import("std");
const Allocator = std.mem.Allocator;
const net = std.net;
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const codec = @import("codec.zig");
const conn_pool_mod = @import("conn_pool.zig");
const log = @import("log.zig");
const Packet = pkt_mod.Packet;
const ConnPool = conn_pool_mod.ConnPool;

const WORKER_COUNT: usize = 16;
const MAX_QUEUE_SIZE: usize = 256;
const MAX_FOLLOWERS: usize = 8;
const REPLICATION_TIMEOUT_NS: u64 = 10 * std.time.ns_per_s;

/// Result of a replication operation across followers.
pub const ReplicationResult = struct {
    success_count: usize,
    failure_count: usize,
    errors: [MAX_FOLLOWERS]?ReplicationError,

    pub fn isOk(self: *const ReplicationResult) bool {
        return self.failure_count == 0;
    }

    pub fn init() ReplicationResult {
        return .{
            .success_count = 0,
            .failure_count = 0,
            .errors = .{null} ** MAX_FOLLOWERS,
        };
    }
};

pub const ReplicationError = struct {
    follower_idx: usize,
    addr: []const u8,
    kind: ErrorKind,
};

pub const ErrorKind = enum {
    connect_failed,
    send_failed,
    recv_failed,
    follower_rejected,
    enqueue_failed,
};

/// A forward task queued for execution by the thread pool.
const ForwardTask = struct {
    pkt: *const Packet,
    addr: []const u8,
    remaining_addrs: []const []const u8,
    pool: *ConnPool,
    allocator: Allocator,
    result: *bool,
    done: *std.Thread.ResetEvent,
};

/// Bounded work queue with mutex + condition variable.
const WorkQueue = struct {
    tasks: std.ArrayList(ForwardTask),
    mu: std.Thread.Mutex,
    not_empty: std.Thread.Condition,
    shutdown: bool,

    fn init(allocator: Allocator) WorkQueue {
        return .{
            .tasks = std.ArrayList(ForwardTask).init(allocator),
            .mu = .{},
            .not_empty = .{},
            .shutdown = false,
        };
    }

    fn deinit(self: *WorkQueue) void {
        self.tasks.deinit();
    }

    fn push(self: *WorkQueue, task: ForwardTask) bool {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.shutdown or self.tasks.items.len >= MAX_QUEUE_SIZE) {
            return false;
        }

        self.tasks.append(task) catch return false;
        self.not_empty.signal();
        return true;
    }

    fn pop(self: *WorkQueue) ?ForwardTask {
        self.mu.lock();
        defer self.mu.unlock();

        while (self.tasks.items.len == 0 and !self.shutdown) {
            self.not_empty.wait(&self.mu);
        }

        if (self.shutdown and self.tasks.items.len == 0) return null;

        if (self.tasks.items.len > 0) {
            // Pop from front (FIFO)
            const task = self.tasks.items[0];
            _ = self.tasks.orderedRemove(0);
            return task;
        }
        return null;
    }

    fn signalShutdown(self: *WorkQueue) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.shutdown = true;
        self.not_empty.broadcast();
    }
};

pub const ReplicationPipeline = struct {
    pool: ConnPool,
    allocator: Allocator,
    queue: WorkQueue,
    workers: [WORKER_COUNT]?std.Thread,
    shutdown: std.atomic.Value(bool),

    pub fn init(allocator: Allocator) ReplicationPipeline {
        return ReplicationPipeline{
            .pool = ConnPool.init(allocator),
            .allocator = allocator,
            .queue = WorkQueue.init(allocator),
            .workers = .{null} ** WORKER_COUNT,
            .shutdown = std.atomic.Value(bool).init(false),
        };
    }

    /// Start the worker threads and connection pool cleanup.
    /// Must be called after the pipeline is at its final memory location.
    pub fn start(self: *ReplicationPipeline) void {
        self.pool.startCleanup();
        for (&self.workers) |*w| {
            w.* = std.Thread.spawn(.{}, workerLoop, .{&self.queue}) catch null;
        }
    }

    pub fn deinit(self: *ReplicationPipeline) void {
        self.shutdown.store(true, .release);
        self.queue.signalShutdown();

        for (&self.workers) |*w| {
            if (w.*) |t| {
                t.join();
                w.* = null;
            }
        }

        self.queue.deinit();
        self.pool.deinit();
    }

    /// Replicate an operation to followers using chain replication.
    /// Forwards to the FIRST follower only, passing remaining follower
    /// addresses so each node in the chain forwards to the next.
    /// Works for writes, create-extent, mark-delete, etc.
    /// Returns a ReplicationResult with the outcome.
    pub fn replicateOp(
        self: *ReplicationPipeline,
        pkt: *const Packet,
        local_result: *Packet,
        allocator: Allocator,
    ) ReplicationResult {
        var repl_result = ReplicationResult.init();

        if (pkt.arg.len == 0 or pkt.remaining_followers == 0) {
            return repl_result;
        }

        const addrs = pkt_mod.splitFollowerAddrs(pkt.arg, allocator) catch return repl_result;
        defer allocator.free(addrs);

        if (addrs.len == 0) return repl_result;

        // Chain replication: forward to the FIRST follower only.
        // Include the remaining addresses so it can forward to the next hop.
        var result: bool = false;
        var event = std.Thread.ResetEvent{};

        const queued = self.queue.push(.{
            .pkt = pkt,
            .addr = addrs[0],
            .remaining_addrs = if (addrs.len > 1) addrs[1..] else &.{},
            .pool = &self.pool,
            .allocator = allocator,
            .result = &result,
            .done = &event,
        });

        if (!queued) {
            repl_result.errors[0] = .{
                .follower_idx = 0,
                .addr = "",
                .kind = .enqueue_failed,
            };
            repl_result.failure_count = 1;
            log.warn("replication enqueue failed for req_id={d}", .{pkt.req_id});
            return repl_result;
        }

        // Wait for completion with timeout to avoid blocking the handler thread forever
        event.timedWait(REPLICATION_TIMEOUT_NS) catch {
            log.warn("replication timeout for req_id={d} to {s}", .{ pkt.req_id, addrs[0] });
            repl_result.errors[0] = .{
                .follower_idx = 0,
                .addr = addrs[0],
                .kind = .recv_failed,
            };
            repl_result.failure_count = 1;
            return repl_result;
        };

        if (result) {
            repl_result.success_count = 1;
        } else {
            repl_result.errors[0] = .{
                .follower_idx = 0,
                .addr = addrs[0],
                .kind = .follower_rejected,
            };
            repl_result.failure_count = 1;
            log.warn("follower forward failed for req_id={d}", .{pkt.req_id});
        }

        // If local write failed, mark it in the result
        if (local_result.result_code != proto.OP_OK) {
            log.warn("local write failed for req_id={d}, code={d}", .{ pkt.req_id, local_result.result_code });
        }

        return repl_result;
    }
};

fn workerLoop(queue: *WorkQueue) void {
    while (true) {
        const task = queue.pop() orelse return;
        task.result.* = forwardToFollower(task.pkt, task.addr, task.remaining_addrs, task.pool, task.allocator);
        task.done.set();
    }
}

fn forwardToFollower(
    pkt: *const Packet,
    addr: []const u8,
    remaining_addrs: []const []const u8,
    pool: *ConnPool,
    allocator: Allocator,
) bool {
    // Get or create connection
    const stream = pool.get(addr) catch return false;

    const writer = stream.writer();
    const reader = stream.reader();

    // Build forwarded packet
    var fwd = Packet{
        .magic = pkt.magic,
        .extent_type = pkt.extent_type,
        .opcode = pkt.opcode,
        .result_code = pkt.result_code,
        .remaining_followers = if (remaining_addrs.len > 0) @intCast(remaining_addrs.len) else 0,
        .crc = pkt.crc,
        .size = pkt.size,
        .partition_id = pkt.partition_id,
        .extent_id = pkt.extent_id,
        .extent_offset = pkt.extent_offset,
        .req_id = pkt.req_id,
        .kernel_offset = pkt.kernel_offset,
        .ver_seq = pkt.ver_seq,
        .proto_version = pkt.proto_version,
        .data = pkt.data,
    };

    // Encode remaining follower addresses as arg.
    // IMPORTANT: allocated_arg must outlive writePacket since fwd.arg points to it.
    var allocated_arg: ?[]u8 = null;
    defer if (allocated_arg) |a| allocator.free(a);

    if (remaining_addrs.len > 0) {
        const new_arg = pkt_mod.encodeFollowerAddrs(remaining_addrs, allocator) catch {
            stream.close();
            return false;
        };
        fwd.arg = new_arg;
        fwd.arg_len = @intCast(new_arg.len);
        allocated_arg = new_arg;
    }

    // Send
    codec.writePacket(writer, &fwd) catch {
        stream.close();
        return false;
    };

    // Read response
    var resp = codec.readPacket(reader, allocator) catch {
        stream.close();
        return false;
    };
    defer resp.deinit();

    // Check response
    if (resp.result_code != proto.OP_OK) {
        pool.put(addr, stream);
        return false;
    }

    // Return connection to pool
    pool.put(addr, stream);
    return true;
}

// ── Tests ────────────────────────────────────────────────────────────

const QueueConsumerCtx = struct {
    queue: *WorkQueue,
    ready: *std.atomic.Value(bool),
    popped_ids: []i64,
    popped_count: *std.atomic.Value(usize),
    saw_shutdown: *std.atomic.Value(bool),
};

fn queueConsumer(ctx: QueueConsumerCtx) void {
    ctx.ready.store(true, .release);

    while (true) {
        const task = ctx.queue.pop() orelse {
            ctx.saw_shutdown.store(true, .release);
            return;
        };

        const idx = ctx.popped_count.fetchAdd(1, .acq_rel);
        if (idx < ctx.popped_ids.len) {
            ctx.popped_ids[idx] = task.pkt.req_id;
        }
    }
}

test "work queue push and pop" {
    var queue = WorkQueue.init(std.testing.allocator);
    defer queue.deinit();

    var result: bool = false;
    var event = std.Thread.ResetEvent{};
    const pkt = Packet{ .opcode = proto.OP_WRITE, .req_id = 42 };

    try std.testing.expect(queue.push(.{
        .pkt = &pkt,
        .addr = "127.0.0.1:1234",
        .remaining_addrs = &.{},
        .pool = undefined,
        .allocator = std.testing.allocator,
        .result = &result,
        .done = &event,
    }));

    const task = queue.pop();
    try std.testing.expect(task != null);
    try std.testing.expectEqual(@as(i64, 42), task.?.pkt.req_id);
}

test "work queue preserves FIFO across multiple tasks" {
    var queue = WorkQueue.init(std.testing.allocator);
    defer queue.deinit();

    var results = [_]bool{ false, false, false };
    var events = [_]std.Thread.ResetEvent{ .{}, .{}, .{} };
    var packets = [_]Packet{
        .{ .opcode = proto.OP_WRITE, .req_id = 1 },
        .{ .opcode = proto.OP_WRITE, .req_id = 2 },
        .{ .opcode = proto.OP_WRITE, .req_id = 3 },
    };

    for (&packets, 0..) |*pkt, i| {
        try std.testing.expect(queue.push(.{
            .pkt = pkt,
            .addr = "127.0.0.1:1234",
            .remaining_addrs = &.{},
            .pool = undefined,
            .allocator = std.testing.allocator,
            .result = &results[i],
            .done = &events[i],
        }));
    }

    const first = queue.pop();
    const second = queue.pop();
    const third = queue.pop();

    try std.testing.expect(first != null);
    try std.testing.expect(second != null);
    try std.testing.expect(third != null);
    try std.testing.expectEqual(@as(i64, 1), first.?.pkt.req_id);
    try std.testing.expectEqual(@as(i64, 2), second.?.pkt.req_id);
    try std.testing.expectEqual(@as(i64, 3), third.?.pkt.req_id);
}

test "work queue shutdown" {
    var queue = WorkQueue.init(std.testing.allocator);
    defer queue.deinit();

    queue.signalShutdown();
    const task = queue.pop();
    try std.testing.expect(task == null);
}

test "work queue shutdown still drains queued tasks" {
    var queue = WorkQueue.init(std.testing.allocator);
    defer queue.deinit();

    var result: bool = false;
    var event = std.Thread.ResetEvent{};
    const pkt = Packet{ .opcode = proto.OP_WRITE, .req_id = 99 };

    try std.testing.expect(queue.push(.{
        .pkt = &pkt,
        .addr = "127.0.0.1:1234",
        .remaining_addrs = &.{},
        .pool = undefined,
        .allocator = std.testing.allocator,
        .result = &result,
        .done = &event,
    }));

    queue.signalShutdown();

    const first = queue.pop();
    const second = queue.pop();

    try std.testing.expect(first != null);
    try std.testing.expectEqual(@as(i64, 99), first.?.pkt.req_id);
    try std.testing.expect(second == null);
}

test "work queue idle consumer exits after shutdown signal" {
    var queue = WorkQueue.init(std.testing.allocator);
    defer queue.deinit();

    var ready = std.atomic.Value(bool).init(false);
    var popped_ids = [_]i64{0} ** 1;
    var popped_count = std.atomic.Value(usize).init(0);
    var saw_shutdown = std.atomic.Value(bool).init(false);

    const consumer = try std.Thread.spawn(.{}, queueConsumer, .{QueueConsumerCtx{
        .queue = &queue,
        .ready = &ready,
        .popped_ids = popped_ids[0..],
        .popped_count = &popped_count,
        .saw_shutdown = &saw_shutdown,
    }});

    while (!ready.load(.acquire)) {
        std.time.sleep(100 * std.time.ns_per_us);
    }

    std.time.sleep(1 * std.time.ns_per_ms);

    queue.signalShutdown();
    consumer.join();

    try std.testing.expect(saw_shutdown.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), popped_count.load(.acquire));
}

test "work queue live consumer drains queued tasks before exit" {
    var queue = WorkQueue.init(std.testing.allocator);
    defer queue.deinit();

    var ready = std.atomic.Value(bool).init(false);
    var popped_ids = [_]i64{ 0, 0 };
    var popped_count = std.atomic.Value(usize).init(0);
    var saw_shutdown = std.atomic.Value(bool).init(false);

    const consumer = try std.Thread.spawn(.{}, queueConsumer, .{QueueConsumerCtx{
        .queue = &queue,
        .ready = &ready,
        .popped_ids = popped_ids[0..],
        .popped_count = &popped_count,
        .saw_shutdown = &saw_shutdown,
    }});

    while (!ready.load(.acquire)) {
        std.time.sleep(100 * std.time.ns_per_us);
    }

    var results = [_]bool{ false, false };
    var events = [_]std.Thread.ResetEvent{ .{}, .{} };
    var packets = [_]Packet{
        .{ .opcode = proto.OP_WRITE, .req_id = 11 },
        .{ .opcode = proto.OP_WRITE, .req_id = 12 },
    };

    for (&packets, 0..) |*pkt, i| {
        try std.testing.expect(queue.push(.{
            .pkt = pkt,
            .addr = "127.0.0.1:1234",
            .remaining_addrs = &.{},
            .pool = undefined,
            .allocator = std.testing.allocator,
            .result = &results[i],
            .done = &events[i],
        }));
    }

    queue.signalShutdown();
    consumer.join();

    try std.testing.expect(saw_shutdown.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), popped_count.load(.acquire));
    try std.testing.expectEqual(@as(i64, 11), popped_ids[0]);
    try std.testing.expectEqual(@as(i64, 12), popped_ids[1]);
}

test "work queue rejects pushes past configured capacity" {
    var queue = WorkQueue.init(std.testing.allocator);
    defer queue.deinit();

    var result: bool = false;
    var event = std.Thread.ResetEvent{};
    const pkt = Packet{ .opcode = proto.OP_WRITE, .req_id = 777 };

    for (0..MAX_QUEUE_SIZE) |_| {
        try std.testing.expect(queue.push(.{
            .pkt = &pkt,
            .addr = "127.0.0.1:1234",
            .remaining_addrs = &.{},
            .pool = undefined,
            .allocator = std.testing.allocator,
            .result = &result,
            .done = &event,
        }));
    }

    try std.testing.expect(!queue.push(.{
        .pkt = &pkt,
        .addr = "127.0.0.1:1234",
        .remaining_addrs = &.{},
        .pool = undefined,
        .allocator = std.testing.allocator,
        .result = &result,
        .done = &event,
    }));
}

test "replication result init" {
    const result = ReplicationResult.init();
    try std.testing.expectEqual(@as(usize, 0), result.success_count);
    try std.testing.expectEqual(@as(usize, 0), result.failure_count);
    try std.testing.expect(result.isOk());
}
