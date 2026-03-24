// server.zig — RaftServer: manages multiple partition Raft instances + tick loop.
//
// Each partition gets its own RaftInstance with independent WAL and state.
// A single background tick thread drives all instances.

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_iface = @import("../raft.zig");
const raft_types = @import("types.zig");
const raft_msgs = @import("messages.zig");
const raft_core = @import("core.zig");
const raft_instance = @import("raft_instance.zig");
const transport_mod = @import("transport.zig");
const log = @import("../log.zig");

const RaftInstance = raft_instance.RaftInstance;
const RaftConfig = raft_types.RaftConfig;
const RaftTransport = transport_mod.RaftTransport;
const Message = raft_msgs.Message;
const MessageHandler = transport_mod.MessageHandler;

pub const ServerConfig = struct {
    node_id: u64,
    heartbeat_addr: []const u8,
    heartbeat_port: u16,
    tick_interval_ms: u32 = 300,
    wal_dir: []const u8,
    election_timeout_ms: u32 = 1000,
    retain_logs: u64 = 20000,
};

pub const RaftServer = struct {
    instances: std.AutoHashMap(u64, *RaftInstance),
    transport: RaftTransport,
    tick_thread: ?std.Thread,
    running: std.atomic.Value(bool),
    tick_interval_ms: u32,
    config: ServerConfig,
    allocator: Allocator,
    mu: std.Thread.Mutex,

    /// Node address registry: node_id → (address, heartbeat_port, replica_port).
    nodes: std.AutoHashMap(u64, NodeInfo),

    const NodeInfo = struct {
        address: []const u8,
        heartbeat_port: u16,
        replica_port: u16,
    };

    pub fn init(allocator: Allocator, config: ServerConfig) !*RaftServer {
        const self = try allocator.create(RaftServer);
        self.* = .{
            .instances = std.AutoHashMap(u64, *RaftInstance).init(allocator),
            .transport = RaftTransport.init(allocator),
            .tick_thread = null,
            .running = std.atomic.Value(bool).init(false),
            .tick_interval_ms = config.tick_interval_ms,
            .config = config,
            .allocator = allocator,
            .mu = .{},
            .nodes = std.AutoHashMap(u64, NodeInfo).init(allocator),
        };

        // Set self as the transport message handler
        self.transport.setHandler(.{
            .ptr = @ptrCast(self),
            .vtable = &handler_vtable,
        });

        return self;
    }

    pub fn deinit(self: *RaftServer) void {
        self.stop();

        // Clean up all instances
        var it = self.instances.valueIterator();
        while (it.next()) |inst| {
            inst.*.deinit();
        }
        self.instances.deinit();
        self.nodes.deinit();
        self.transport.deinit();
        self.allocator.destroy(self);
    }

    /// Start the raft server: transport listener + tick loop.
    pub fn start(self: *RaftServer) !void {
        if (self.running.load(.acquire)) return;
        self.running.store(true, .release);

        // Start transport listener
        self.transport.startListener(
            self.config.heartbeat_addr,
            self.config.heartbeat_port,
        ) catch |e| {
            log.warn("raft: failed to start transport listener: {}", .{e});
        };

        // Start tick loop
        self.tick_thread = std.Thread.spawn(.{}, tickLoop, .{self}) catch {
            log.warn("raft: failed to spawn tick thread", .{});
            return;
        };

        log.info("raft server started: node={d}, heartbeat={s}:{d}", .{
            self.config.node_id,
            self.config.heartbeat_addr,
            self.config.heartbeat_port,
        });
    }

    /// Stop the raft server.
    pub fn stop(self: *RaftServer) void {
        self.running.store(false, .release);

        self.transport.stop();

        if (self.tick_thread) |t| {
            t.join();
            self.tick_thread = null;
        }
    }

    /// Create a Raft partition and return its instance.
    pub fn createPartition(
        self: *RaftServer,
        partition_id: u64,
        peers: []const raft_iface.Peer,
        sm: raft_iface.StateMachine,
    ) !*RaftInstance {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.instances.contains(partition_id)) {
            return error.ExtentExists;
        }

        const cfg = RaftConfig{
            .node_id = self.config.node_id,
            .election_timeout_ms = self.config.election_timeout_ms,
            .heartbeat_interval_ms = self.tick_interval_ms,
            .retain_logs = self.config.retain_logs,
            .wal_dir = self.config.wal_dir,
        };

        const instance = try RaftInstance.init(
            self.allocator,
            partition_id,
            cfg,
            peers,
            sm,
            &self.transport,
        );

        try self.instances.put(partition_id, instance);

        log.info("raft: created partition {d} with {d} peers", .{ partition_id, peers.len });
        return instance;
    }

    /// Stop and remove a partition.
    pub fn stopPartition(self: *RaftServer, partition_id: u64) void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.instances.fetchRemove(partition_id)) |kv| {
            kv.value.deinit();
            log.info("raft: stopped partition {d}", .{partition_id});
        }
    }

    /// Get a partition instance by ID.
    pub fn getPartition(self: *RaftServer, partition_id: u64) ?*RaftInstance {
        self.mu.lock();
        defer self.mu.unlock();
        return self.instances.get(partition_id);
    }

    /// Register a node's address for message routing.
    pub fn addNode(self: *RaftServer, node_id: u64, addr: []const u8, heartbeat_port: u16, replica_port: u16) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.nodes.put(node_id, .{
            .address = addr,
            .heartbeat_port = heartbeat_port,
            .replica_port = replica_port,
        }) catch {};
    }

    /// Handle an incoming message for a partition (called by transport).
    fn handleIncomingMessage(ctx: *anyopaque, partition_id: u64, data: []const u8, allocator: Allocator) void {
        const self: *RaftServer = @ptrCast(@alignCast(ctx));

        const msg = raft_msgs.decode(allocator, data) catch return;
        // Note: msg may have allocated entries, need to free after handling

        self.mu.lock();
        const instance = self.instances.get(partition_id);
        self.mu.unlock();

        if (instance) |inst| {
            inst.handleMessage(&msg);
        }

        // Free decoded entries if present
        if (msg.payload == .append_entries) {
            allocator.free(msg.payload.append_entries.entries);
        }
    }

    fn tickLoop(self: *RaftServer) void {
        const interval_ns: u64 = @as(u64, self.tick_interval_ms) * std.time.ns_per_ms;

        while (self.running.load(.acquire)) {
            std.time.sleep(interval_ns);
            if (!self.running.load(.acquire)) break;

            const now_ms = std.time.milliTimestamp();

            self.mu.lock();
            // Collect instance pointers under lock
            var instances_buf: [1024]*RaftInstance = undefined;
            var count: usize = 0;
            var it = self.instances.valueIterator();
            while (it.next()) |inst| {
                if (count < instances_buf.len) {
                    instances_buf[count] = inst.*;
                    count += 1;
                }
            }
            self.mu.unlock();

            // Tick each instance outside the lock
            for (instances_buf[0..count]) |inst| {
                inst.tick(now_ms);
            }
        }
    }

    const handler_vtable = MessageHandler.VTable{
        .handleMessage = handleIncomingMessage,
    };
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

const TestSM = struct {
    applied: std.atomic.Value(u64),
    leader: std.atomic.Value(u64),

    fn init() TestSM {
        return .{
            .applied = std.atomic.Value(u64).init(0),
            .leader = std.atomic.Value(u64).init(0),
        };
    }

    fn applyImpl(ctx: *anyopaque, _: []const u8, _: u64) anyerror!void {
        const self: *TestSM = @ptrCast(@alignCast(ctx));
        _ = self.applied.fetchAdd(1, .seq_cst);
    }
    fn applyMemberChangeImpl(_: *anyopaque, _: raft_iface.ConfChangeType, _: raft_iface.Peer, _: u64) anyerror!void {}
    fn snapshotImpl(_: *anyopaque) u64 { return 0; }
    fn handleLeaderChangeImpl(ctx: *anyopaque, leader_id: u64) void {
        const self: *TestSM = @ptrCast(@alignCast(ctx));
        self.leader.store(leader_id, .release);
    }
    fn handleFatalImpl(_: *anyopaque) void {}

    fn stateMachine(self: *TestSM) raft_iface.StateMachine {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &sm_vtable,
        };
    }

    const sm_vtable = raft_iface.StateMachine.VTable{
        .apply = applyImpl,
        .applyMemberChange = applyMemberChangeImpl,
        .snapshot = snapshotImpl,
        .handleLeaderChange = handleLeaderChangeImpl,
        .handleFatal = handleFatalImpl,
    };
};

test "RaftServer create and get partition" {
    const alloc = testing.allocator;
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const wal_dir = try std.fmt.allocPrint(alloc, "/tmp/zig_raft_srv_{d}", .{timestamp});
    defer alloc.free(wal_dir);
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    std.fs.cwd().makePath(wal_dir) catch {};

    const server = try RaftServer.init(alloc, .{
        .node_id = 1,
        .heartbeat_addr = "127.0.0.1",
        .heartbeat_port = 0, // don't bind
        .wal_dir = wal_dir,
    });
    defer server.deinit();

    var tsm = TestSM.init();
    const inst = try server.createPartition(100, &.{}, tsm.stateMachine());

    // Verify we can get it back
    const got = server.getPartition(100);
    try testing.expect(got != null);
    try testing.expectEqual(@as(u64, 100), inst.partition_id);

    // Stop it
    server.stopPartition(100);
    try testing.expect(server.getPartition(100) == null);
}

test "RaftServer single-node partition submit" {
    const alloc = testing.allocator;
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const wal_dir = try std.fmt.allocPrint(alloc, "/tmp/zig_raft_srv2_{d}", .{timestamp});
    defer alloc.free(wal_dir);
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    std.fs.cwd().makePath(wal_dir) catch {};

    const server = try RaftServer.init(alloc, .{
        .node_id = 1,
        .heartbeat_addr = "127.0.0.1",
        .heartbeat_port = 0,
        .wal_dir = wal_dir,
    });
    defer server.deinit();

    var tsm = TestSM.init();
    const inst = try server.createPartition(200, &.{}, tsm.stateMachine());

    // Trigger election
    inst.tick(std.time.milliTimestamp() + 10000);

    const rp = inst.partition();
    try testing.expect(rp.isLeader());

    // Submit
    try rp.submit("data1");
    try rp.submit("data2");

    try testing.expectEqual(@as(u64, 2), tsm.applied.load(.acquire));
}
