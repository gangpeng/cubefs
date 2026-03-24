// test_cluster.zig — Multi-node test harness for Raft consensus.
//
// Provides a `RaftTestCluster` that manages N RaftCore instances with in-memory
// message routing, network partitioning, and assertion helpers. Inspired by
// tikv/raft-rs `Network` test harness.

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_types = @import("types.zig");
const raft_msgs = @import("messages.zig");
const raft_core = @import("core.zig");
const wal_mod = @import("wal.zig");
const raft_iface = @import("../raft.zig");

const RaftRole = raft_types.RaftRole;
const RaftConfig = raft_types.RaftConfig;
const LogEntry = raft_types.LogEntry;
const Message = raft_msgs.Message;
const MsgType = raft_msgs.MsgType;
const RaftCore = raft_core.RaftCore;
const WalStorage = wal_mod.WalStorage;

// ─── TestStateMachine ───────────────────────────────────────────────

/// Test state machine that records all applied entries and leader changes.
pub const TestStateMachine = struct {
    applied_entries: std.ArrayList(AppliedEntry),
    leader_changes: std.ArrayList(u64),
    fatal_called: bool,
    allocator: Allocator,

    pub const AppliedEntry = struct {
        data: []u8,
        index: u64,
    };

    pub fn init(allocator: Allocator) TestStateMachine {
        return .{
            .applied_entries = std.ArrayList(AppliedEntry).init(allocator),
            .leader_changes = std.ArrayList(u64).init(allocator),
            .fatal_called = false,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TestStateMachine) void {
        for (self.applied_entries.items) |entry| {
            self.allocator.free(entry.data);
        }
        self.applied_entries.deinit();
        self.leader_changes.deinit();
    }

    pub fn appliedCount(self: *const TestStateMachine) usize {
        return self.applied_entries.items.len;
    }

    pub fn lastAppliedIndex(self: *const TestStateMachine) u64 {
        if (self.applied_entries.items.len == 0) return 0;
        return self.applied_entries.items[self.applied_entries.items.len - 1].index;
    }

    pub fn currentLeader(self: *const TestStateMachine) u64 {
        if (self.leader_changes.items.len == 0) return 0;
        return self.leader_changes.items[self.leader_changes.items.len - 1];
    }

    fn applyImpl(ctx: *anyopaque, cmd: []const u8, index: u64) anyerror!void {
        const self: *TestStateMachine = @ptrCast(@alignCast(ctx));
        const owned = try self.allocator.dupe(u8, cmd);
        try self.applied_entries.append(.{ .data = owned, .index = index });
    }

    fn applyMemberChangeImpl(_: *anyopaque, _: raft_iface.ConfChangeType, _: raft_iface.Peer, _: u64) anyerror!void {}

    fn snapshotImpl(ctx: *anyopaque) u64 {
        const self: *TestStateMachine = @ptrCast(@alignCast(ctx));
        return self.lastAppliedIndex();
    }

    fn handleLeaderChangeImpl(ctx: *anyopaque, leader_id: u64) void {
        const self: *TestStateMachine = @ptrCast(@alignCast(ctx));
        self.leader_changes.append(leader_id) catch {};
    }

    fn handleFatalImpl(ctx: *anyopaque) void {
        const self: *TestStateMachine = @ptrCast(@alignCast(ctx));
        self.fatal_called = true;
    }

    pub fn stateMachine(self: *TestStateMachine) raft_iface.StateMachine {
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

// ─── NodeContext ─────────────────────────────────────────────────────

/// Internal struct holding a single node's resources.
const NodeContext = struct {
    core: *RaftCore,
    wal: *WalStorage,
    sm: *TestStateMachine,
    wal_dir: []u8,
};

// ─── RaftTestCluster ─────────────────────────────────────────────────

/// A multi-node test cluster with in-memory message routing.
pub const RaftTestCluster = struct {
    nodes: std.AutoHashMap(u64, NodeContext),
    /// Bidirectional partitions: if (a,b) is in the set, messages a→b are dropped.
    partitions: std.AutoHashMap(u128, void),
    /// Pending messages to deliver.
    pending: std.ArrayList(Message),
    allocator: Allocator,
    /// Simulated clock (milliseconds).
    clock_ms: i64,

    pub fn init(allocator: Allocator, node_count: usize) !RaftTestCluster {
        var cluster = RaftTestCluster{
            .nodes = std.AutoHashMap(u64, NodeContext).init(allocator),
            .partitions = std.AutoHashMap(u128, void).init(allocator),
            .pending = std.ArrayList(Message).init(allocator),
            .allocator = allocator,
            .clock_ms = 1000, // Start at 1s so timers fire naturally
        };

        // Create nodes 1..N
        var i: u64 = 1;
        while (i <= node_count) : (i += 1) {
            try cluster.addNode(i);
        }

        // Wire peers: every node knows about every other node
        var it = cluster.nodes.iterator();
        while (it.next()) |kv| {
            const node_id = kv.key_ptr.*;
            var pit = cluster.nodes.iterator();
            while (pit.next()) |pkv| {
                const peer_id = pkv.key_ptr.*;
                if (peer_id != node_id) {
                    try kv.value_ptr.core.addPeer(.{
                        .id = peer_id,
                        .address = "127.0.0.1",
                        .heartbeat_port = 17330,
                        .replica_port = 17340,
                    });
                }
            }
        }

        return cluster;
    }

    fn addNode(self: *RaftTestCluster, node_id: u64) !void {
        const timestamp = @as(u64, @intCast(std.time.nanoTimestamp())) +% node_id;
        const wal_dir = try std.fmt.allocPrint(self.allocator, "/tmp/zig_raft_cluster_test_{d}_{d}", .{ timestamp, node_id });

        const wal = try WalStorage.open(self.allocator, wal_dir);

        const sm = try self.allocator.create(TestStateMachine);
        sm.* = TestStateMachine.init(self.allocator);

        const cfg = RaftConfig{
            .node_id = node_id,
            .wal_dir = wal_dir,
            .election_timeout_ms = 150,
            .heartbeat_interval_ms = 50,
        };

        const core = try RaftCore.init(self.allocator, cfg, wal, sm.stateMachine());

        try self.nodes.put(node_id, .{
            .core = core,
            .wal = wal,
            .sm = sm,
            .wal_dir = wal_dir,
        });
    }

    pub fn deinit(self: *RaftTestCluster) void {
        // Free pending messages
        for (self.pending.items) |msg| {
            self.freeMessagePayload(msg);
        }
        self.pending.deinit();

        var it = self.nodes.iterator();
        while (it.next()) |kv| {
            const ctx = kv.value_ptr;
            ctx.core.deinit();
            ctx.wal.close();
            ctx.sm.deinit();
            self.allocator.destroy(ctx.sm);
            std.fs.cwd().deleteTree(ctx.wal_dir) catch {};
            self.allocator.free(ctx.wal_dir);
        }
        self.nodes.deinit();
        self.partitions.deinit();
    }

    pub fn freeMessagePayload(self: *RaftTestCluster, msg: Message) void {
        if (msg.payload == .append_entries) {
            const entries = msg.payload.append_entries.entries;
            if (entries.len > 0) {
                self.allocator.free(entries);
            }
        }
    }

    /// Get a node's RaftCore.
    pub fn getNode(self: *RaftTestCluster, id: u64) *RaftCore {
        return self.nodes.get(id).?.core;
    }

    /// Get a node's TestStateMachine.
    pub fn getStateMachine(self: *RaftTestCluster, id: u64) *TestStateMachine {
        return self.nodes.get(id).?.sm;
    }

    /// Tick a single node.
    pub fn tickNode(self: *RaftTestCluster, id: u64) !void {
        const node = self.getNode(id);
        if (try node.tick(self.clock_ms)) |msgs| {
            for (msgs) |msg| {
                try self.pending.append(msg);
            }
            self.allocator.free(msgs);
        }
    }

    /// Advance the simulated clock.
    pub fn advanceClock(self: *RaftTestCluster, ms: i64) void {
        self.clock_ms += ms;
    }

    /// Force a node to start an election by advancing past its deadline.
    pub fn triggerElection(self: *RaftTestCluster, id: u64) !void {
        const node = self.getNode(id);
        // Set clock well past the election deadline
        self.clock_ms = node.election_deadline_ms + 1;
        try self.tickNode(id);
    }

    /// Deliver all pending messages, routing through partition rules.
    /// Repeats until no more messages are generated (quiescence).
    pub fn deliverMessages(self: *RaftTestCluster) !void {
        var rounds: usize = 0;
        while (self.pending.items.len > 0 and rounds < 100) : (rounds += 1) {
            // Swap pending to a local copy
            var current = std.ArrayList(Message).init(self.allocator);
            std.mem.swap(std.ArrayList(Message), &self.pending, &current);
            defer current.deinit();

            for (current.items) |msg| {
                // Check partition
                if (self.isPartitioned(msg.from, msg.to)) {
                    self.freeMessagePayload(msg);
                    continue;
                }

                const node_ctx = self.nodes.getPtr(msg.to);
                if (node_ctx == null) {
                    self.freeMessagePayload(msg);
                    continue;
                }
                const node = node_ctx.?.core;

                if (try node.handleMessage(&msg)) |replies| {
                    for (replies) |reply| {
                        try self.pending.append(reply);
                    }
                    self.allocator.free(replies);
                }

                self.freeMessagePayload(msg);
            }
        }
    }

    /// Create a bidirectional partition between two nodes.
    pub fn cut(self: *RaftTestCluster, a: u64, b: u64) !void {
        try self.partitions.put(partitionKey(a, b), {});
        try self.partitions.put(partitionKey(b, a), {});
    }

    /// Isolate a node from all others.
    pub fn isolate(self: *RaftTestCluster, id: u64) !void {
        var it = self.nodes.keyIterator();
        while (it.next()) |key_ptr| {
            const other = key_ptr.*;
            if (other != id) {
                try self.cut(id, other);
            }
        }
    }

    /// Remove all partition rules.
    pub fn recover(self: *RaftTestCluster) void {
        self.partitions.clearRetainingCapacity();
    }

    /// Elect a leader by triggering election on node 1 and delivering messages.
    pub fn electLeader(self: *RaftTestCluster) !u64 {
        try self.triggerElection(1);
        try self.deliverMessages();
        return self.findLeader();
    }

    /// Submit data to the current leader and deliver until committed.
    pub fn submitAndCommit(self: *RaftTestCluster, data: []const u8) !void {
        const leader_id = self.findLeader();
        if (leader_id == 0) return error.NotLeader;

        const leader = self.getNode(leader_id);
        try leader.submit(data);

        // Send AppendEntries and deliver responses
        if (try leader.tick(self.clock_ms)) |msgs| {
            for (msgs) |msg| {
                try self.pending.append(msg);
            }
            self.allocator.free(msgs);
        }
        // Also broadcast explicitly
        const broadcast = try leader.broadcastAppendEntries();
        for (broadcast) |msg| {
            try self.pending.append(msg);
        }
        self.allocator.free(broadcast);

        try self.deliverMessages();
    }

    /// Find the current leader (returns 0 if none).
    pub fn findLeader(self: *RaftTestCluster) u64 {
        var it = self.nodes.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.core.role == .leader) {
                return kv.key_ptr.*;
            }
        }
        return 0;
    }

    // ─── Assertion Helpers ──────────────────────────────────────────

    /// Assert exactly one leader exists in the cluster.
    pub fn expectOneLeader(self: *RaftTestCluster) !u64 {
        var leader_count: usize = 0;
        var leader_id: u64 = 0;
        var it = self.nodes.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.core.role == .leader) {
                leader_count += 1;
                leader_id = kv.key_ptr.*;
            }
        }
        try std.testing.expectEqual(@as(usize, 1), leader_count);
        return leader_id;
    }

    /// Assert a node has a specific role.
    pub fn expectRole(self: *RaftTestCluster, id: u64, expected_role: RaftRole) !void {
        const node = self.getNode(id);
        try std.testing.expectEqual(expected_role, node.role);
    }

    /// Assert all non-partitioned nodes have consistent logs
    /// (same entries up to the minimum commit_index).
    pub fn expectLogsConsistent(self: *RaftTestCluster) !void {
        // Find the leader's commit_index as reference
        const leader_id = self.findLeader();
        if (leader_id == 0) return; // No leader, skip

        const leader = self.getNode(leader_id);
        const commit = leader.commit_index;
        if (commit == 0) return;

        var it = self.nodes.iterator();
        while (it.next()) |kv| {
            const node_id = kv.key_ptr.*;
            if (self.isPartitioned(leader_id, node_id)) continue;
            if (node_id == leader_id) continue;

            const node = kv.value_ptr.core;
            const check_to = @min(commit, node.commit_index);

            var idx: u64 = 1;
            while (idx <= check_to) : (idx += 1) {
                const leader_term = leader.log.termAt(idx);
                const node_term = node.log.termAt(idx);
                if (leader_term == 0 or node_term == 0) continue;
                try std.testing.expectEqual(leader_term, node_term);
            }
        }
    }

    /// Assert no node has the given role.
    pub fn expectNoRole(self: *RaftTestCluster, role: RaftRole) !void {
        var it = self.nodes.iterator();
        while (it.next()) |kv| {
            try std.testing.expect(kv.value_ptr.core.role != role);
        }
    }

    /// Count nodes with the given role.
    pub fn countRole(self: *RaftTestCluster, role: RaftRole) usize {
        var count: usize = 0;
        var it = self.nodes.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.core.role == role) {
                count += 1;
            }
        }
        return count;
    }

    /// Broadcast a heartbeat from the leader and deliver.
    pub fn heartbeat(self: *RaftTestCluster) !void {
        const leader_id = self.findLeader();
        if (leader_id == 0) return;
        const leader = self.getNode(leader_id);
        // Force heartbeat by advancing past heartbeat deadline
        self.clock_ms = leader.heartbeat_deadline_ms + 1;
        try self.tickNode(leader_id);
        try self.deliverMessages();
    }

    // ─── Internal ───────────────────────────────────────────────────

    fn isPartitioned(self: *const RaftTestCluster, from: u64, to: u64) bool {
        return self.partitions.contains(partitionKey(from, to));
    }

    fn partitionKey(a: u64, b: u64) u128 {
        return (@as(u128, a) << 64) | @as(u128, b);
    }
};

// ─── Self-test ──────────────────────────────────────────────────────

const testing = std.testing;

test "TestCluster basic 3-node election" {
    var cluster = try RaftTestCluster.init(testing.allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.electLeader();
    try testing.expect(leader != 0);
    _ = try cluster.expectOneLeader();
}

test "TestCluster submit and commit" {
    var cluster = try RaftTestCluster.init(testing.allocator, 3);
    defer cluster.deinit();

    _ = try cluster.electLeader();
    try cluster.submitAndCommit("hello");

    // Leader should have applied it
    const leader_id = cluster.findLeader();
    const sm = cluster.getStateMachine(leader_id);
    try testing.expect(sm.appliedCount() > 0);
}
