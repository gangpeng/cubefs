// types.zig — Core types and constants for the pure-Zig Raft implementation.

const std = @import("std");

/// Raft node role in the consensus protocol.
pub const RaftRole = enum(u8) {
    follower = 0,
    candidate = 1,
    leader = 2,
};

/// Type of a Raft log entry.
pub const EntryType = enum(u8) {
    /// Normal application data.
    normal = 0,
    /// Membership configuration change.
    conf_change = 1,
    /// No-op entry (committed on leader election to advance commit index).
    noop = 2,
};

/// A single Raft log entry.
pub const LogEntry = struct {
    term: u64,
    index: u64,
    entry_type: EntryType = .normal,
    data: []const u8 = &.{},
};

/// Configuration for a Raft partition instance.
pub const RaftConfig = struct {
    node_id: u64,
    /// Base election timeout in milliseconds. Actual timeout is randomized
    /// between [election_timeout_ms, 2 * election_timeout_ms).
    election_timeout_ms: u32 = 1000,
    /// Heartbeat interval in milliseconds (must be < election_timeout_ms).
    heartbeat_interval_ms: u32 = 300,
    /// Maximum entries per AppendEntries RPC.
    max_entries_per_append: u32 = 64,
    /// Number of log entries to retain before truncation.
    retain_logs: u64 = 20000,
    /// Directory for WAL segment files.
    wal_dir: []const u8,
};

/// Tracks the replication state for a single peer.
pub const PeerState = struct {
    id: u64,
    address: []const u8,
    heartbeat_port: u16,
    replica_port: u16,
    /// Next log index to send to this peer.
    next_index: u64 = 1,
    /// Highest log index known to be replicated on this peer.
    match_index: u64 = 0,
    /// Whether this peer responded to recent RPCs.
    active: bool = true,
};

/// Membership change type for conf_change entries.
pub const ConfChangeType = enum(u8) {
    add_node = 0,
    remove_node = 1,
};

/// Conf change payload stored in a conf_change LogEntry.
pub const ConfChange = struct {
    change_type: ConfChangeType,
    node_id: u64,
    address: []const u8 = &.{},
    heartbeat_port: u16 = 0,
    replica_port: u16 = 0,
};

/// Callback/future for tracking pending submissions awaiting commit.
pub const PendingSubmit = struct {
    index: u64,
    term: u64,
};

/// No peer ID (used for voted_for when no vote cast).
pub const NONE: u64 = 0;

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "RaftRole enum values" {
    try testing.expectEqual(@as(u8, 0), @intFromEnum(RaftRole.follower));
    try testing.expectEqual(@as(u8, 1), @intFromEnum(RaftRole.candidate));
    try testing.expectEqual(@as(u8, 2), @intFromEnum(RaftRole.leader));
}

test "EntryType enum values" {
    try testing.expectEqual(@as(u8, 0), @intFromEnum(EntryType.normal));
    try testing.expectEqual(@as(u8, 1), @intFromEnum(EntryType.conf_change));
    try testing.expectEqual(@as(u8, 2), @intFromEnum(EntryType.noop));
}

test "LogEntry default fields" {
    const entry = LogEntry{ .term = 1, .index = 1 };
    try testing.expectEqual(EntryType.normal, entry.entry_type);
    try testing.expectEqual(@as(usize, 0), entry.data.len);
}

test "RaftConfig defaults" {
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = "/tmp/wal" };
    try testing.expectEqual(@as(u32, 1000), cfg.election_timeout_ms);
    try testing.expectEqual(@as(u32, 300), cfg.heartbeat_interval_ms);
    try testing.expectEqual(@as(u32, 64), cfg.max_entries_per_append);
    try testing.expectEqual(@as(u64, 20000), cfg.retain_logs);
}

test "PeerState defaults" {
    const ps = PeerState{ .id = 2, .address = "10.0.0.2", .heartbeat_port = 17330, .replica_port = 17340 };
    try testing.expectEqual(@as(u64, 1), ps.next_index);
    try testing.expectEqual(@as(u64, 0), ps.match_index);
    try testing.expect(ps.active);
}
