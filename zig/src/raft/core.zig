// core.zig — Raft consensus state machine: leader election, log replication,
// commit advancement.
//
// Implements the Raft protocol as described in the Raft paper (Ongaro & Ousterhout).
// This is a pure state machine: it takes inputs (tick, message) and returns
// outputs (messages to send). No I/O or threads — the caller drives the loop.

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_types = @import("types.zig");
pub const raft_msgs = @import("messages.zig");
const wal_mod = @import("wal.zig");
const raft_iface = @import("../raft.zig");

const RaftRole = raft_types.RaftRole;
const LogEntry = raft_types.LogEntry;
const EntryType = raft_types.EntryType;
const PeerState = raft_types.PeerState;
const RaftConfig = raft_types.RaftConfig;
const PendingSubmit = raft_types.PendingSubmit;
const ConfChangeType = raft_types.ConfChangeType;
const Message = raft_msgs.Message;
const MsgType = raft_msgs.MsgType;
const Payload = raft_msgs.Payload;
const WalStorage = wal_mod.WalStorage;

pub const RaftCore = struct {
    // ─── Persistent state ────────────────────────────────────────
    current_term: u64,
    voted_for: u64, // 0 = none
    log: *WalStorage,

    // ─── Volatile state ──────────────────────────────────────────
    role: RaftRole,
    commit_index: u64,
    last_applied: u64,
    leader_id: u64, // 0 = unknown
    node_id: u64,

    // ─── Leader state ────────────────────────────────────────────
    peers: std.AutoHashMap(u64, PeerState),

    // ─── Election state ──────────────────────────────────────────
    votes_received: std.AutoHashMap(u64, bool),
    election_deadline_ms: i64,
    heartbeat_deadline_ms: i64,

    // ─── Config ──────────────────────────────────────────────────
    config: RaftConfig,

    // ─── State machine callback ──────────────────────────────────
    state_machine: raft_iface.StateMachine,

    // ─── Pending submissions ─────────────────────────────────────
    pending: std.ArrayList(PendingSubmit),

    allocator: Allocator,

    /// Initialize a new RaftCore.
    pub fn init(
        allocator: Allocator,
        config: RaftConfig,
        wal: *WalStorage,
        sm: raft_iface.StateMachine,
    ) !*RaftCore {
        const self = try allocator.create(RaftCore);
        self.* = .{
            .current_term = 0,
            .voted_for = raft_types.NONE,
            .log = wal,
            .role = .follower,
            .commit_index = 0,
            .last_applied = 0,
            .leader_id = raft_types.NONE,
            .node_id = config.node_id,
            .peers = std.AutoHashMap(u64, PeerState).init(allocator),
            .votes_received = std.AutoHashMap(u64, bool).init(allocator),
            .election_deadline_ms = 0,
            .heartbeat_deadline_ms = 0,
            .config = config,
            .state_machine = sm,
            .pending = std.ArrayList(PendingSubmit).init(allocator),
            .allocator = allocator,
        };

        // Restore term from WAL if possible
        if (wal.lastTerm() > 0) {
            self.current_term = wal.lastTerm();
        }

        return self;
    }

    pub fn deinit(self: *RaftCore) void {
        self.peers.deinit();
        self.votes_received.deinit();
        self.pending.deinit();
        self.allocator.destroy(self);
    }

    /// Add a peer to the cluster.
    pub fn addPeer(self: *RaftCore, peer: PeerState) !void {
        try self.peers.put(peer.id, peer);
    }

    /// Remove a peer from the cluster.
    pub fn removePeer(self: *RaftCore, peer_id: u64) void {
        _ = self.peers.remove(peer_id);
    }

    /// Called periodically (every tick_interval_ms). Returns messages to send.
    pub fn tick(self: *RaftCore, now_ms: i64) !?[]Message {
        return switch (self.role) {
            .follower, .candidate => self.tickElection(now_ms),
            .leader => self.tickHeartbeat(now_ms),
        };
    }

    /// Handle an incoming RPC message. Returns response messages to send.
    pub fn handleMessage(self: *RaftCore, msg: *const Message) !?[]Message {
        // If the message has a higher term, step down
        if (msg.term > self.current_term) {
            self.becomeFollower(msg.term, raft_types.NONE);
        }

        return switch (msg.msg_type) {
            .request_vote => try self.handleRequestVote(msg),
            .request_vote_response => try self.handleRequestVoteResponse(msg),
            .append_entries => try self.handleAppendEntries(msg),
            .append_entries_response => try self.handleAppendEntriesResponse(msg),
            .heartbeat => try self.handleHeartbeat(msg),
            .heartbeat_response => null,
        };
    }

    /// Submit a command for consensus (leader only).
    pub fn submit(self: *RaftCore, data: []const u8) !void {
        if (self.role != .leader) return error.NotLeader;

        const new_index = self.log.lastIndex() + 1;
        const entry = LogEntry{
            .term = self.current_term,
            .index = new_index,
            .entry_type = .normal,
            .data = data,
        };

        try self.log.append(entry);
        try self.pending.append(.{ .index = new_index, .term = self.current_term });

        // If single-node cluster, commit immediately
        if (self.peers.count() == 0) {
            self.commit_index = new_index;
            try self.applyCommitted();
        }
    }

    /// Submit a membership change (leader only).
    pub fn changeMember(self: *RaftCore, ct: ConfChangeType, peer: PeerState) !void {
        if (self.role != .leader) return error.NotLeader;

        const new_index = self.log.lastIndex() + 1;
        const entry = LogEntry{
            .term = self.current_term,
            .index = new_index,
            .entry_type = .conf_change,
            .data = &.{}, // Simplified: actual implementation would serialize peer info
        };

        try self.log.append(entry);

        // Apply membership change immediately (joint consensus simplified)
        switch (ct) {
            .add_node => try self.addPeer(peer),
            .remove_node => self.removePeer(peer.id),
        }

        // Notify state machine
        const iface_peer = raft_iface.Peer{
            .id = peer.id,
            .address = peer.address,
            .heartbeat_port = peer.heartbeat_port,
            .replica_port = peer.replica_port,
        };
        const iface_ct: raft_iface.ConfChangeType = switch (ct) {
            .add_node => .add_node,
            .remove_node => .remove_node,
        };
        try self.state_machine.applyMemberChange(iface_ct, iface_peer, new_index);
    }

    /// Whether this node is the leader.
    pub fn isLeader(self: *const RaftCore) bool {
        return self.role == .leader;
    }

    /// The last applied log index.
    pub fn appliedIndex(self: *const RaftCore) u64 {
        return self.last_applied;
    }

    /// The current leader term.
    pub fn leaderTerm(self: *const RaftCore) u64 {
        if (self.role == .leader) return self.current_term;
        return 0;
    }

    // ─── Raft protocol internals ─────────────────────────────────

    fn becomeFollower(self: *RaftCore, term: u64, leader: u64) void {
        self.role = .follower;
        self.current_term = term;
        self.voted_for = raft_types.NONE;
        self.leader_id = leader;
        self.state_machine.handleLeaderChange(leader);
    }

    fn becomeCandidate(self: *RaftCore, now_ms: i64) !?[]Message {
        self.role = .candidate;
        self.current_term += 1;
        self.voted_for = self.node_id;
        self.leader_id = raft_types.NONE;

        // Vote for self
        self.votes_received.clearRetainingCapacity();
        try self.votes_received.put(self.node_id, true);

        self.resetElectionTimer(now_ms);

        // If single-node cluster, become leader immediately
        if (self.peers.count() == 0) {
            return try self.becomeLeader(now_ms);
        }

        // Send RequestVote to all peers
        var msgs = std.ArrayList(Message).init(self.allocator);
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            const peer_id = kv.key_ptr.*;
            try msgs.append(.{
                .msg_type = .request_vote,
                .term = self.current_term,
                .from = self.node_id,
                .to = peer_id,
                .partition_id = 0,
                .payload = .{ .request_vote = .{
                    .last_log_index = self.log.lastIndex(),
                    .last_log_term = self.log.lastTerm(),
                } },
            });
        }

        const result = try msgs.toOwnedSlice();
        return result;
    }

    fn becomeLeader(self: *RaftCore, now_ms: i64) !?[]Message {
        self.role = .leader;
        self.leader_id = self.node_id;
        self.state_machine.handleLeaderChange(self.node_id);

        // Reinitialize peer replication state
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            kv.value_ptr.next_index = self.log.lastIndex() + 1;
            kv.value_ptr.match_index = 0;
        }

        // Append a no-op entry to commit entries from previous terms
        const noop = LogEntry{
            .term = self.current_term,
            .index = self.log.lastIndex() + 1,
            .entry_type = .noop,
            .data = &.{},
        };
        try self.log.append(noop);

        // If single-node, commit the noop
        if (self.peers.count() == 0) {
            self.commit_index = self.log.lastIndex();
            try self.applyCommitted();
        }

        self.heartbeat_deadline_ms = now_ms;

        // Send initial empty AppendEntries (heartbeat) to all peers
        return try self.broadcastAppendEntries();
    }

    fn handleRequestVote(self: *RaftCore, msg: *const Message) !?[]Message {
        const rv = msg.payload.request_vote;
        var grant = false;

        if (msg.term >= self.current_term) {
            // Grant vote if we haven't voted yet (or voted for the same candidate)
            // and the candidate's log is at least as up-to-date as ours
            const can_vote = (self.voted_for == raft_types.NONE or self.voted_for == msg.from);
            const log_ok = self.isLogUpToDate(rv.last_log_index, rv.last_log_term);

            if (can_vote and log_ok) {
                self.voted_for = msg.from;
                grant = true;
            }
        }

        const reply = Message{
            .msg_type = .request_vote_response,
            .term = self.current_term,
            .from = self.node_id,
            .to = msg.from,
            .partition_id = msg.partition_id,
            .payload = .{ .request_vote_response = .{ .vote_granted = grant } },
        };

        var msgs = try self.allocator.alloc(Message, 1);
        msgs[0] = reply;
        return msgs;
    }

    fn handleRequestVoteResponse(self: *RaftCore, msg: *const Message) !?[]Message {
        if (self.role != .candidate) return null;
        if (msg.term != self.current_term) return null;

        const rvr = msg.payload.request_vote_response;
        try self.votes_received.put(msg.from, rvr.vote_granted);

        // Count votes
        var granted: usize = 0;
        var vit = self.votes_received.valueIterator();
        while (vit.next()) |v| {
            if (v.*) granted += 1;
        }

        if (granted >= self.quorumSize()) {
            return try self.becomeLeader(std.time.milliTimestamp());
        }

        return null;
    }

    fn handleAppendEntries(self: *RaftCore, msg: *const Message) !?[]Message {
        const ae = msg.payload.append_entries;
        self.leader_id = msg.from;

        // Reset election timer since we heard from the leader
        self.resetElectionTimer(std.time.milliTimestamp());

        // If we're a candidate and receive from a valid leader, step down
        if (self.role == .candidate) {
            self.becomeFollower(msg.term, msg.from);
        }

        // Check log consistency: do we have the prev entry?
        if (ae.prev_log_index > 0) {
            const prev_term = self.log.termAt(ae.prev_log_index);
            if (prev_term != ae.prev_log_term) {
                // Log inconsistency — reject
                const reply = Message{
                    .msg_type = .append_entries_response,
                    .term = self.current_term,
                    .from = self.node_id,
                    .to = msg.from,
                    .partition_id = msg.partition_id,
                    .payload = .{ .append_entries_response = .{
                        .success = false,
                        .match_index = 0,
                    } },
                };
                var msgs = try self.allocator.alloc(Message, 1);
                msgs[0] = reply;
                return msgs;
            }
        }

        // Append new entries (handle conflicts by truncating)
        for (ae.entries) |entry| {
            const existing_term = self.log.termAt(entry.index);
            if (existing_term != 0 and existing_term != entry.term) {
                // Conflict: delete this and all following entries
                try self.log.truncateAfter(entry.index - 1);
            }

            if (entry.index > self.log.lastIndex()) {
                try self.log.append(entry);
            }
        }

        // Update commit index
        if (ae.leader_commit > self.commit_index) {
            self.commit_index = @min(ae.leader_commit, self.log.lastIndex());
            try self.applyCommitted();
        }

        const reply = Message{
            .msg_type = .append_entries_response,
            .term = self.current_term,
            .from = self.node_id,
            .to = msg.from,
            .partition_id = msg.partition_id,
            .payload = .{ .append_entries_response = .{
                .success = true,
                .match_index = self.log.lastIndex(),
            } },
        };
        var msgs = try self.allocator.alloc(Message, 1);
        msgs[0] = reply;
        return msgs;
    }

    fn handleAppendEntriesResponse(self: *RaftCore, msg: *const Message) !?[]Message {
        if (self.role != .leader) return null;

        const aer = msg.payload.append_entries_response;
        const peer = self.peers.getPtr(msg.from) orelse return null;

        if (aer.success) {
            peer.match_index = aer.match_index;
            peer.next_index = aer.match_index + 1;
            peer.active = true;

            // Try to advance commit index
            try self.advanceCommitIndex();
        } else {
            // Decrement nextIndex and retry
            if (peer.next_index > 1) {
                peer.next_index -= 1;
            }
            // Send entries starting from new next_index
            const append_msg = try self.buildAppendEntries(msg.from);
            if (append_msg) |m| {
                var msgs = try self.allocator.alloc(Message, 1);
                msgs[0] = m;
                return msgs;
            }
        }

        return null;
    }

    fn handleHeartbeat(self: *RaftCore, msg: *const Message) !?[]Message {
        self.leader_id = msg.from;
        self.resetElectionTimer(std.time.milliTimestamp());

        if (self.role == .candidate) {
            self.becomeFollower(msg.term, msg.from);
        }

        const hb = msg.payload.heartbeat;
        if (hb.leader_commit > self.commit_index) {
            self.commit_index = @min(hb.leader_commit, self.log.lastIndex());
            try self.applyCommitted();
        }

        const reply = Message{
            .msg_type = .heartbeat_response,
            .term = self.current_term,
            .from = self.node_id,
            .to = msg.from,
            .partition_id = msg.partition_id,
            .payload = .{ .heartbeat_response = .{
                .match_index = self.log.lastIndex(),
            } },
        };
        var msgs = try self.allocator.alloc(Message, 1);
        msgs[0] = reply;
        return msgs;
    }

    fn advanceCommitIndex(self: *RaftCore) !void {
        // Find the highest index N such that:
        // - N > commit_index
        // - A majority of peers have match_index >= N
        // - log[N].term == current_term
        var n = self.log.lastIndex();
        while (n > self.commit_index) : (n -= 1) {
            if (self.log.termAt(n) != self.current_term) continue;

            var match_count: usize = 1; // count self
            var pit = self.peers.valueIterator();
            while (pit.next()) |peer| {
                if (peer.match_index >= n) {
                    match_count += 1;
                }
            }

            if (match_count >= self.quorumSize()) {
                self.commit_index = n;
                try self.applyCommitted();
                return;
            }

            if (n == 0) break;
        }
    }

    fn applyCommitted(self: *RaftCore) !void {
        while (self.last_applied < self.commit_index) {
            self.last_applied += 1;
            const entry = self.log.getEntry(self.last_applied) catch continue;

            switch (entry.entry_type) {
                .normal => {
                    if (entry.data.len > 0) {
                        self.state_machine.apply(entry.data, self.last_applied) catch {};
                    }
                },
                .conf_change => {
                    // Already applied during changeMember
                },
                .noop => {
                    // No-op entries don't need application
                },
            }
        }
    }

    fn tickElection(self: *RaftCore, now_ms: i64) !?[]Message {
        if (now_ms >= self.election_deadline_ms) {
            return try self.becomeCandidate(now_ms);
        }
        return null;
    }

    fn tickHeartbeat(self: *RaftCore, now_ms: i64) !?[]Message {
        if (now_ms >= self.heartbeat_deadline_ms) {
            self.heartbeat_deadline_ms = now_ms + self.config.heartbeat_interval_ms;
            return try self.broadcastAppendEntries();
        }
        return null;
    }

    pub fn broadcastAppendEntries(self: *RaftCore) ![]Message {
        var msgs = std.ArrayList(Message).init(self.allocator);

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            const peer_id = kv.key_ptr.*;
            if (try self.buildAppendEntries(peer_id)) |msg| {
                try msgs.append(msg);
            }
        }

        return msgs.toOwnedSlice();
    }

    fn buildAppendEntries(self: *RaftCore, peer_id: u64) !?Message {
        const peer = self.peers.getPtr(peer_id) orelse return null;

        const prev_log_index = if (peer.next_index > 0) peer.next_index - 1 else 0;
        const prev_log_term = self.log.termAt(prev_log_index);

        // Get entries to send
        const first = peer.next_index;
        const last_avail = self.log.lastIndex();
        var entries: []const LogEntry = &.{};

        if (first <= last_avail) {
            const hi = @min(first + self.config.max_entries_per_append, last_avail + 1);
            entries = self.log.getEntries(first, hi, self.allocator) catch &.{};
        }

        if (entries.len == 0) {
            // Send heartbeat-style empty AppendEntries
            return Message{
                .msg_type = .heartbeat,
                .term = self.current_term,
                .from = self.node_id,
                .to = peer_id,
                .partition_id = 0,
                .payload = .{ .heartbeat = .{
                    .leader_commit = self.commit_index,
                } },
            };
        }

        return Message{
            .msg_type = .append_entries,
            .term = self.current_term,
            .from = self.node_id,
            .to = peer_id,
            .partition_id = 0,
            .payload = .{ .append_entries = .{
                .prev_log_index = prev_log_index,
                .prev_log_term = prev_log_term,
                .leader_commit = self.commit_index,
                .entries = entries,
            } },
        };
    }

    fn isLogUpToDate(self: *const RaftCore, last_index: u64, last_term: u64) bool {
        const my_last_term = self.log.lastTerm();
        if (last_term != my_last_term) {
            return last_term > my_last_term;
        }
        return last_index >= self.log.lastIndex();
    }

    fn resetElectionTimer(self: *RaftCore, now_ms: i64) void {
        // Randomize: [election_timeout, 2 * election_timeout)
        const base = self.config.election_timeout_ms;
        // Simple deterministic "randomization" based on node_id and current_term
        const jitter = (self.node_id *% 7 +% self.current_term *% 13) % base;
        self.election_deadline_ms = now_ms + @as(i64, @intCast(base + jitter));
    }

    fn quorumSize(self: *const RaftCore) usize {
        const total = self.peers.count() + 1; // +1 for self
        return total / 2 + 1;
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

/// Test state machine that records apply calls.
const TestStateMachine = struct {
    applied_count: std.atomic.Value(u64),
    last_index: std.atomic.Value(u64),
    leader_id: std.atomic.Value(u64),

    fn init() TestStateMachine {
        return .{
            .applied_count = std.atomic.Value(u64).init(0),
            .last_index = std.atomic.Value(u64).init(0),
            .leader_id = std.atomic.Value(u64).init(0),
        };
    }

    fn applyImpl(ctx: *anyopaque, _: []const u8, index: u64) anyerror!void {
        const self: *TestStateMachine = @ptrCast(@alignCast(ctx));
        _ = self.applied_count.fetchAdd(1, .seq_cst);
        self.last_index.store(index, .release);
    }

    fn applyMemberChangeImpl(_: *anyopaque, _: raft_iface.ConfChangeType, _: raft_iface.Peer, _: u64) anyerror!void {}

    fn snapshotImpl(ctx: *anyopaque) u64 {
        const self: *TestStateMachine = @ptrCast(@alignCast(ctx));
        return self.last_index.load(.acquire);
    }

    fn handleLeaderChangeImpl(ctx: *anyopaque, leader_id: u64) void {
        const self: *TestStateMachine = @ptrCast(@alignCast(ctx));
        self.leader_id.store(leader_id, .release);
    }

    fn handleFatalImpl(_: *anyopaque) void {}

    fn stateMachine(self: *TestStateMachine) raft_iface.StateMachine {
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

fn makeTempDir(allocator: Allocator) ![]u8 {
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    return std.fmt.allocPrint(allocator, "/tmp/zig_raft_test_{d}", .{timestamp});
}

test "RaftCore init starts as follower" {
    const alloc = testing.allocator;
    const dir = try makeTempDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    var tsm = TestStateMachine.init();
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = dir };

    const core = try RaftCore.init(alloc, cfg, wal, tsm.stateMachine());
    defer {
        core.deinit();
        wal.close();
    }

    try testing.expectEqual(RaftRole.follower, core.role);
    try testing.expectEqual(@as(u64, 0), core.current_term);
    try testing.expect(!core.isLeader());
}

test "RaftCore single-node becomes leader on election timeout" {
    const alloc = testing.allocator;
    const dir = try makeTempDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    var tsm = TestStateMachine.init();
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = dir, .election_timeout_ms = 100 };

    const core = try RaftCore.init(alloc, cfg, wal, tsm.stateMachine());
    defer {
        core.deinit();
        wal.close();
    }

    // Trigger election by ticking past deadline
    const now = std.time.milliTimestamp();
    const far_future = now + 10000;
    const msgs = try core.tick(far_future);
    if (msgs) |m| alloc.free(m);

    // Single-node cluster: should become leader immediately
    try testing.expectEqual(RaftRole.leader, core.role);
    try testing.expect(core.isLeader());
    try testing.expectEqual(@as(u64, 1), core.current_term);
}

test "RaftCore single-node submit and apply" {
    const alloc = testing.allocator;
    const dir = try makeTempDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    var tsm = TestStateMachine.init();
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = dir, .election_timeout_ms = 100 };

    const core = try RaftCore.init(alloc, cfg, wal, tsm.stateMachine());
    defer {
        core.deinit();
        wal.close();
    }

    // Become leader
    const far_future = std.time.milliTimestamp() + 10000;
    const msgs = try core.tick(far_future);
    if (msgs) |m| alloc.free(m);

    // Submit commands
    try core.submit("cmd1");
    try core.submit("cmd2");
    try core.submit("cmd3");

    try testing.expectEqual(@as(u64, 3), tsm.applied_count.load(.acquire));
    try testing.expectEqual(@as(u64, 1), tsm.leader_id.load(.acquire));
}

test "RaftCore follower rejects submit" {
    const alloc = testing.allocator;
    const dir = try makeTempDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    var tsm = TestStateMachine.init();
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = dir };

    const core = try RaftCore.init(alloc, cfg, wal, tsm.stateMachine());
    defer {
        core.deinit();
        wal.close();
    }

    try testing.expectError(error.NotLeader, core.submit("cmd"));
}

test "RaftCore multi-node election with RequestVote" {
    const alloc = testing.allocator;
    const dir = try makeTempDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    var tsm = TestStateMachine.init();
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = dir, .election_timeout_ms = 100 };

    const core = try RaftCore.init(alloc, cfg, wal, tsm.stateMachine());
    defer {
        core.deinit();
        wal.close();
    }

    // Add peers (3-node cluster)
    try core.addPeer(.{ .id = 2, .address = "10.0.0.2", .heartbeat_port = 17330, .replica_port = 17340 });
    try core.addPeer(.{ .id = 3, .address = "10.0.0.3", .heartbeat_port = 17330, .replica_port = 17340 });

    // Trigger election
    const far_future = std.time.milliTimestamp() + 10000;
    const vote_reqs = try core.tick(far_future);

    // Should be candidate, sending RequestVote to 2 peers
    try testing.expectEqual(RaftRole.candidate, core.role);
    if (vote_reqs) |reqs| {
        defer alloc.free(reqs);
        try testing.expectEqual(@as(usize, 2), reqs.len);
        try testing.expectEqual(MsgType.request_vote, reqs[0].msg_type);
    }

    // Simulate vote granted from peer 2
    const vote_response = Message{
        .msg_type = .request_vote_response,
        .term = core.current_term,
        .from = 2,
        .to = 1,
        .partition_id = 0,
        .payload = .{ .request_vote_response = .{ .vote_granted = true } },
    };
    const result = try core.handleMessage(&vote_response);
    if (result) |r| {
        for (r) |msg| {
            if (msg.payload == .append_entries) {
                alloc.free(msg.payload.append_entries.entries);
            }
        }
        alloc.free(r);
    }

    // Should become leader with quorum (self + peer2 = 2 out of 3)
    try testing.expectEqual(RaftRole.leader, core.role);
}

test "RaftCore leader sends AppendEntries on submit" {
    const alloc = testing.allocator;
    const dir = try makeTempDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    var tsm = TestStateMachine.init();
    const cfg = RaftConfig{ .node_id = 1, .wal_dir = dir, .election_timeout_ms = 100, .heartbeat_interval_ms = 50 };

    const core = try RaftCore.init(alloc, cfg, wal, tsm.stateMachine());
    defer {
        core.deinit();
        wal.close();
    }

    // Add one peer (2-node cluster)
    try core.addPeer(.{ .id = 2, .address = "10.0.0.2", .heartbeat_port = 17330, .replica_port = 17340 });

    // Become candidate
    const far_future = std.time.milliTimestamp() + 10000;
    const vote_reqs = try core.tick(far_future);
    if (vote_reqs) |r| alloc.free(r);

    // Get vote from peer
    const vote = Message{
        .msg_type = .request_vote_response,
        .term = core.current_term,
        .from = 2,
        .to = 1,
        .partition_id = 0,
        .payload = .{ .request_vote_response = .{ .vote_granted = true } },
    };
    const leader_msgs = try core.handleMessage(&vote);
    if (leader_msgs) |m| {
        // These are the initial AppendEntries after becoming leader
        for (m) |msg| {
            if (msg.payload == .append_entries) {
                alloc.free(msg.payload.append_entries.entries);
            }
        }
        alloc.free(m);
    }

    try testing.expectEqual(RaftRole.leader, core.role);

    // Submit a command
    try core.submit("test_data");

    // Data not committed yet (need peer response)
    try testing.expectEqual(@as(u64, 0), tsm.applied_count.load(.acquire));

    // Simulate AppendEntries response from peer
    const ae_resp = Message{
        .msg_type = .append_entries_response,
        .term = core.current_term,
        .from = 2,
        .to = 1,
        .partition_id = 0,
        .payload = .{ .append_entries_response = .{
            .success = true,
            .match_index = core.log.lastIndex(),
        } },
    };
    const result = try core.handleMessage(&ae_resp);
    if (result) |r| alloc.free(r);

    // Now it should be committed and applied
    try testing.expect(tsm.applied_count.load(.acquire) > 0);
}
