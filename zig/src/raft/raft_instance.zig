// raft_instance.zig — Per-partition Raft wrapper.
//
// Wraps RaftCore + WAL to implement the RaftPartition vtable from src/raft.zig.
// Each DataPartition gets one RaftInstance.

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_iface = @import("../raft.zig");
const raft_types = @import("types.zig");
const raft_core = @import("core.zig");
const wal_mod = @import("wal.zig");
const transport_mod = @import("transport.zig");

const RaftCore = raft_core.RaftCore;
const WalStorage = wal_mod.WalStorage;
const RaftConfig = raft_types.RaftConfig;
const PeerState = raft_types.PeerState;
const RaftTransport = transport_mod.RaftTransport;

pub const RaftInstance = struct {
    core: *RaftCore,
    wal: *WalStorage,
    partition_id: u64,
    transport: *RaftTransport,
    allocator: Allocator,
    mu: std.Thread.Mutex,
    stopped: std.atomic.Value(bool),

    /// Create a new RaftInstance for a partition.
    pub fn init(
        allocator: Allocator,
        partition_id: u64,
        config: RaftConfig,
        peers: []const raft_iface.Peer,
        sm: raft_iface.StateMachine,
        transport: *RaftTransport,
    ) !*RaftInstance {
        // Create WAL directory for this partition
        var wal_path_buf: [4096]u8 = undefined;
        const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}/wal_{d}", .{ config.wal_dir, partition_id }) catch {
            return error.InvalidData;
        };
        const owned_wal_path = try allocator.dupe(u8, wal_path);

        const wal = WalStorage.open(allocator, owned_wal_path) catch {
            allocator.free(owned_wal_path);
            return error.InvalidData;
        };

        const core = RaftCore.init(allocator, config, wal, sm) catch {
            wal.close();
            allocator.free(owned_wal_path);
            return error.InvalidData;
        };

        // Add peers to the core
        for (peers) |peer| {
            core.addPeer(.{
                .id = peer.id,
                .address = peer.address,
                .heartbeat_port = peer.heartbeat_port,
                .replica_port = peer.replica_port,
            }) catch {};
        }

        const self = try allocator.create(RaftInstance);
        self.* = .{
            .core = core,
            .wal = wal,
            .partition_id = partition_id,
            .transport = transport,
            .allocator = allocator,
            .mu = .{},
            .stopped = std.atomic.Value(bool).init(false),
        };

        return self;
    }

    pub fn deinit(self: *RaftInstance) void {
        self.core.deinit();
        const wal_path = self.wal.dir_path;
        self.wal.close();
        self.allocator.free(wal_path);
        self.allocator.destroy(self);
    }

    /// Get a RaftPartition interface backed by this instance.
    pub fn partition(self: *RaftInstance) raft_iface.RaftPartition {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    /// Tick the underlying Raft core and dispatch any resulting messages.
    pub fn tick(self: *RaftInstance, now_ms: i64) void {
        if (self.stopped.load(.acquire)) return;

        self.mu.lock();
        defer self.mu.unlock();

        const msgs = self.core.tick(now_ms) catch return;
        if (msgs) |messages| {
            defer self.allocator.free(messages);
            self.dispatchMessages(messages);
        }
    }

    /// Handle an incoming message for this partition.
    pub fn handleMessage(self: *RaftInstance, msg: *const raft_core.raft_msgs.Message) void {
        if (self.stopped.load(.acquire)) return;

        self.mu.lock();
        defer self.mu.unlock();

        const responses = self.core.handleMessage(msg) catch return;
        if (responses) |msgs| {
            defer self.allocator.free(msgs);
            self.dispatchMessages(msgs);
        }
    }

    fn dispatchMessages(self: *RaftInstance, messages: []const raft_core.raft_msgs.Message) void {
        for (messages) |msg| {
            // Find peer address from core
            if (self.core.peers.get(msg.to)) |peer| {
                self.transport.sendMessage(peer.address, peer.heartbeat_port, &msg) catch {};
            }

            // Free any owned entry data in append_entries messages
            if (msg.payload == .append_entries) {
                self.allocator.free(msg.payload.append_entries.entries);
            }
        }
    }

    // ─── VTable implementations ──────────────────────────────────

    fn submitImpl(ctx: *anyopaque, cmd: []const u8) anyerror!void {
        const self: *RaftInstance = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return error.NotLeader;

        self.mu.lock();
        defer self.mu.unlock();

        try self.core.submit(cmd);
    }

    fn changeMemberImpl(ctx: *anyopaque, ct: raft_iface.ConfChangeType, peer: raft_iface.Peer) anyerror!void {
        const self: *RaftInstance = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return error.NotLeader;

        self.mu.lock();
        defer self.mu.unlock();

        const raft_ct: raft_types.ConfChangeType = switch (ct) {
            .add_node => .add_node,
            .remove_node => .remove_node,
        };

        try self.core.changeMember(raft_ct, .{
            .id = peer.id,
            .address = peer.address,
            .heartbeat_port = peer.heartbeat_port,
            .replica_port = peer.replica_port,
        });
    }

    fn stopImpl(ctx: *anyopaque) void {
        const self: *RaftInstance = @ptrCast(@alignCast(ctx));
        self.stopped.store(true, .release);
    }

    fn isLeaderImpl(ctx: *anyopaque) bool {
        const self: *RaftInstance = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return false;
        return self.core.isLeader();
    }

    fn appliedIndexImpl(ctx: *anyopaque) u64 {
        const self: *RaftInstance = @ptrCast(@alignCast(ctx));
        return self.core.appliedIndex();
    }

    fn leaderTermImpl(ctx: *anyopaque) u64 {
        const self: *RaftInstance = @ptrCast(@alignCast(ctx));
        return self.core.leaderTerm();
    }

    const vtable = raft_iface.RaftPartition.VTable{
        .submit = submitImpl,
        .changeMember = changeMemberImpl,
        .stop = stopImpl,
        .isLeader = isLeaderImpl,
        .appliedIndex = appliedIndexImpl,
        .leaderTerm = leaderTermImpl,
    };
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

/// Test state machine.
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

test "RaftInstance single-node submit via vtable" {
    const alloc = testing.allocator;
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const wal_dir = try std.fmt.allocPrint(alloc, "/tmp/zig_ri_test_{d}", .{timestamp});
    defer alloc.free(wal_dir);
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    std.fs.cwd().makePath(wal_dir) catch {};

    var transport = transport_mod.RaftTransport.init(alloc);
    defer transport.deinit();

    var tsm = TestSM.init();
    const cfg = RaftConfig{
        .node_id = 1,
        .wal_dir = wal_dir,
        .election_timeout_ms = 100,
    };

    const instance = try RaftInstance.init(alloc, 42, cfg, &.{}, tsm.stateMachine(), &transport);
    defer instance.deinit();

    const rp = instance.partition();

    // Trigger election (single-node → leader)
    instance.tick(std.time.milliTimestamp() + 10000);
    try testing.expect(rp.isLeader());

    // Submit via vtable
    try rp.submit("hello");
    try rp.submit("world");

    try testing.expectEqual(@as(u64, 2), tsm.applied.load(.acquire));
    try testing.expectEqual(@as(u64, 1), tsm.leader.load(.acquire));
}

test "RaftInstance stop prevents submit" {
    const alloc = testing.allocator;
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const wal_dir = try std.fmt.allocPrint(alloc, "/tmp/zig_ri_test2_{d}", .{timestamp});
    defer alloc.free(wal_dir);
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    std.fs.cwd().makePath(wal_dir) catch {};

    var transport = transport_mod.RaftTransport.init(alloc);
    defer transport.deinit();

    var tsm = TestSM.init();
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = wal_dir, .election_timeout_ms = 100 };

    const instance = try RaftInstance.init(alloc, 43, cfg, &.{}, tsm.stateMachine(), &transport);
    defer instance.deinit();

    const rp = instance.partition();

    // Become leader
    instance.tick(std.time.milliTimestamp() + 10000);
    try testing.expect(rp.isLeader());

    // Stop
    rp.stop();
    try testing.expect(!rp.isLeader());

    // Submit should fail
    try testing.expectError(error.NotLeader, rp.submit("blocked"));
}
