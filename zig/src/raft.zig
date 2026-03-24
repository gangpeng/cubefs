// raft.zig — Raft abstraction layer: interfaces and types.
// Defines vtable-based interfaces so DataPartition can work with any Raft
// implementation (tiglabs/raft via C FFI, pure-Zig raft, or a stub).

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Raft membership change type.
pub const ConfChangeType = enum(u8) {
    add_node = 0,
    remove_node = 1,
};

/// A Raft peer (replica).
pub const Peer = struct {
    id: u64,
    address: []const u8,
    heartbeat_port: u16,
    replica_port: u16,
};

/// Raft partition interface — vtable-based for runtime polymorphism.
/// Wraps a concrete Raft implementation (stub, tiglabs FFI, pure-Zig, etc.).
pub const RaftPartition = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit: *const fn (ctx: *anyopaque, cmd: []const u8) anyerror!void,
        changeMember: *const fn (ctx: *anyopaque, ct: ConfChangeType, peer: Peer) anyerror!void,
        stop: *const fn (ctx: *anyopaque) void,
        isLeader: *const fn (ctx: *anyopaque) bool,
        appliedIndex: *const fn (ctx: *anyopaque) u64,
        leaderTerm: *const fn (ctx: *anyopaque) u64,
    };

    /// Submit a command through Raft consensus.
    pub fn submit(self: *const RaftPartition, cmd: []const u8) !void {
        return self.vtable.submit(self.ptr, cmd);
    }

    /// Request a membership change.
    pub fn changeMember(self: *const RaftPartition, ct: ConfChangeType, peer: Peer) !void {
        return self.vtable.changeMember(self.ptr, ct, peer);
    }

    /// Stop the Raft instance.
    pub fn stop(self: *const RaftPartition) void {
        self.vtable.stop(self.ptr);
    }

    /// Whether this node is the current Raft leader.
    pub fn isLeader(self: *const RaftPartition) bool {
        return self.vtable.isLeader(self.ptr);
    }

    /// The last applied Raft log index.
    pub fn appliedIndex(self: *const RaftPartition) u64 {
        return self.vtable.appliedIndex(self.ptr);
    }

    /// The current leader term.
    pub fn leaderTerm(self: *const RaftPartition) u64 {
        return self.vtable.leaderTerm(self.ptr);
    }
};

/// State machine interface — DataPartition implements this.
/// The Raft library calls these methods when log entries are committed.
pub const StateMachine = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        apply: *const fn (ctx: *anyopaque, cmd: []const u8, index: u64) anyerror!void,
        applyMemberChange: *const fn (ctx: *anyopaque, ct: ConfChangeType, peer: Peer, index: u64) anyerror!void,
        snapshot: *const fn (ctx: *anyopaque) u64,
        handleLeaderChange: *const fn (ctx: *anyopaque, leader_id: u64) void,
        handleFatal: *const fn (ctx: *anyopaque) void,
    };

    /// Apply a committed log entry.
    pub fn apply(self: *const StateMachine, cmd: []const u8, index: u64) !void {
        return self.vtable.apply(self.ptr, cmd, index);
    }

    /// Apply a committed membership change.
    pub fn applyMemberChange(self: *const StateMachine, ct: ConfChangeType, peer: Peer, index: u64) !void {
        return self.vtable.applyMemberChange(self.ptr, ct, peer, index);
    }

    /// Return the last applied snapshot index.
    pub fn snapshot(self: *const StateMachine) u64 {
        return self.vtable.snapshot(self.ptr);
    }

    /// Called when the leader changes.
    pub fn handleLeaderChange(self: *const StateMachine, leader_id: u64) void {
        self.vtable.handleLeaderChange(self.ptr, leader_id);
    }

    /// Called on fatal Raft errors.
    pub fn handleFatal(self: *const StateMachine) void {
        self.vtable.handleFatal(self.ptr);
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

test "ConfChangeType values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(ConfChangeType.add_node));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(ConfChangeType.remove_node));
}

test "Peer struct layout" {
    const peer = Peer{
        .id = 1,
        .address = "10.0.0.1",
        .heartbeat_port = 17330,
        .replica_port = 17340,
    };
    try std.testing.expectEqual(@as(u64, 1), peer.id);
    try std.testing.expectEqualStrings("10.0.0.1", peer.address);
    try std.testing.expectEqual(@as(u16, 17330), peer.heartbeat_port);
    try std.testing.expectEqual(@as(u16, 17340), peer.replica_port);
}
