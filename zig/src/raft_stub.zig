// raft_stub.zig — Single-node stub Raft implementation for testing.
// Immediately applies commands locally without WAL or replication.
// Allows the rest of the infrastructure to work without a real Raft library.

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft = @import("raft.zig");
const RaftPartition = raft.RaftPartition;
const StateMachine = raft.StateMachine;
const ConfChangeType = raft.ConfChangeType;
const Peer = raft.Peer;

pub const RaftStub = struct {
    applied_index: std.atomic.Value(u64),
    is_leader: std.atomic.Value(bool),
    term: std.atomic.Value(u64),
    sm: StateMachine,
    allocator: Allocator,

    /// Create a new stub Raft that immediately applies commands via the state machine.
    pub fn init(allocator: Allocator, sm: StateMachine) !*RaftStub {
        const self = try allocator.create(RaftStub);
        self.* = .{
            .applied_index = std.atomic.Value(u64).init(0),
            .is_leader = std.atomic.Value(bool).init(true), // stub is always leader
            .term = std.atomic.Value(u64).init(1),
            .sm = sm,
            .allocator = allocator,
        };
        return self;
    }

    pub fn deinit(self: *RaftStub) void {
        self.allocator.destroy(self);
    }

    /// Get a RaftPartition interface backed by this stub.
    pub fn partition(self: *RaftStub) RaftPartition {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    fn submitImpl(ctx: *anyopaque, cmd: []const u8) anyerror!void {
        const self: *RaftStub = @ptrCast(@alignCast(ctx));
        const new_index = self.applied_index.fetchAdd(1, .seq_cst) + 1;
        try self.sm.apply(cmd, new_index);
    }

    fn changeMemberImpl(ctx: *anyopaque, ct: ConfChangeType, peer: Peer) anyerror!void {
        const self: *RaftStub = @ptrCast(@alignCast(ctx));
        const new_index = self.applied_index.fetchAdd(1, .seq_cst) + 1;
        try self.sm.applyMemberChange(ct, peer, new_index);
    }

    fn stopImpl(ctx: *anyopaque) void {
        const self: *RaftStub = @ptrCast(@alignCast(ctx));
        self.is_leader.store(false, .release);
    }

    fn isLeaderImpl(ctx: *anyopaque) bool {
        const self: *RaftStub = @ptrCast(@alignCast(ctx));
        return self.is_leader.load(.acquire);
    }

    fn appliedIndexImpl(ctx: *anyopaque) u64 {
        const self: *RaftStub = @ptrCast(@alignCast(ctx));
        return self.applied_index.load(.acquire);
    }

    fn leaderTermImpl(ctx: *anyopaque) u64 {
        const self: *RaftStub = @ptrCast(@alignCast(ctx));
        return self.term.load(.acquire);
    }

    const vtable = RaftPartition.VTable{
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

/// Test state machine that records applied commands.
const TestSM = struct {
    applied_count: std.atomic.Value(u64),
    last_index: std.atomic.Value(u64),
    member_changes: std.atomic.Value(u64),

    fn init() TestSM {
        return .{
            .applied_count = std.atomic.Value(u64).init(0),
            .last_index = std.atomic.Value(u64).init(0),
            .member_changes = std.atomic.Value(u64).init(0),
        };
    }

    fn applyImpl(ctx: *anyopaque, _: []const u8, index: u64) anyerror!void {
        const self: *TestSM = @ptrCast(@alignCast(ctx));
        _ = self.applied_count.fetchAdd(1, .seq_cst);
        self.last_index.store(index, .release);
    }

    fn applyMemberChangeImpl(ctx: *anyopaque, _: ConfChangeType, _: Peer, index: u64) anyerror!void {
        const self: *TestSM = @ptrCast(@alignCast(ctx));
        _ = self.member_changes.fetchAdd(1, .seq_cst);
        self.last_index.store(index, .release);
    }

    fn snapshotImpl(ctx: *anyopaque) u64 {
        const self: *TestSM = @ptrCast(@alignCast(ctx));
        return self.last_index.load(.acquire);
    }

    fn handleLeaderChangeImpl(_: *anyopaque, _: u64) void {}
    fn handleFatalImpl(_: *anyopaque) void {}

    fn stateMachine(self: *TestSM) StateMachine {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &sm_vtable,
        };
    }

    const sm_vtable = StateMachine.VTable{
        .apply = applyImpl,
        .applyMemberChange = applyMemberChangeImpl,
        .snapshot = snapshotImpl,
        .handleLeaderChange = handleLeaderChangeImpl,
        .handleFatal = handleFatalImpl,
    };
};

test "stub is always leader initially" {
    var tsm = TestSM.init();
    const stub = try RaftStub.init(testing.allocator, tsm.stateMachine());
    defer stub.deinit();

    const rp = stub.partition();
    try testing.expect(rp.isLeader());
    try testing.expectEqual(@as(u64, 1), rp.leaderTerm());
}

test "stub submit applies immediately" {
    var tsm = TestSM.init();
    const stub = try RaftStub.init(testing.allocator, tsm.stateMachine());
    defer stub.deinit();

    const rp = stub.partition();
    try rp.submit("cmd1");
    try rp.submit("cmd2");
    try rp.submit("cmd3");

    try testing.expectEqual(@as(u64, 3), tsm.applied_count.load(.acquire));
    try testing.expectEqual(@as(u64, 3), rp.appliedIndex());
    try testing.expectEqual(@as(u64, 3), tsm.last_index.load(.acquire));
}

test "stub changeMember applies immediately" {
    var tsm = TestSM.init();
    const stub = try RaftStub.init(testing.allocator, tsm.stateMachine());
    defer stub.deinit();

    const rp = stub.partition();
    const peer = Peer{ .id = 2, .address = "10.0.0.2", .heartbeat_port = 17330, .replica_port = 17340 };
    try rp.changeMember(.add_node, peer);

    try testing.expectEqual(@as(u64, 1), tsm.member_changes.load(.acquire));
    try testing.expectEqual(@as(u64, 1), rp.appliedIndex());
}

test "stub stop sets leader to false" {
    var tsm = TestSM.init();
    const stub = try RaftStub.init(testing.allocator, tsm.stateMachine());
    defer stub.deinit();

    const rp = stub.partition();
    try testing.expect(rp.isLeader());
    rp.stop();
    try testing.expect(!rp.isLeader());
}

test "stub applied_index increments monotonically" {
    var tsm = TestSM.init();
    const stub = try RaftStub.init(testing.allocator, tsm.stateMachine());
    defer stub.deinit();

    const rp = stub.partition();
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        try rp.submit("x");
    }
    try testing.expectEqual(@as(u64, 100), rp.appliedIndex());
}
