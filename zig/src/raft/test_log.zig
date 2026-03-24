// test_log.zig — Log divergence and multi-leader-change tests.
//
// Tests log truncation, divergence reconciliation, empty-log catch-up,
// log compaction, and noop commitment on election.

const std = @import("std");
const testing = std.testing;
const raft_types = @import("types.zig");
const raft_msgs = @import("messages.zig");
const raft_core = @import("core.zig");
const wal_mod = @import("wal.zig");
const cluster_mod = @import("test_cluster.zig");

const RaftRole = raft_types.RaftRole;
const LogEntry = raft_types.LogEntry;
const Message = raft_msgs.Message;
const MsgType = raft_msgs.MsgType;
const RaftCore = raft_core.RaftCore;
const WalStorage = wal_mod.WalStorage;
const RaftTestCluster = cluster_mod.RaftTestCluster;

test "test_log_divergence_simple" {
    // One conflicting entry gets truncated when the real leader syncs
    var cluster = try RaftTestCluster.init(testing.allocator, 3);
    defer cluster.deinit();

    _ = try cluster.electLeader();
    const leader_id = cluster.findLeader();
    const follower_id: u64 = if (leader_id == 1) 2 else 1;

    // Add a conflicting entry directly to follower's log
    const follower = cluster.getNode(follower_id);
    const next_idx = follower.log.lastIndex() + 1;
    try follower.log.append(.{ .term = 99, .index = next_idx, .data = "conflict" });

    // Leader sends correct entries — should truncate the conflict
    try cluster.heartbeat();
    try cluster.deliverMessages();
    try cluster.heartbeat();
    try cluster.deliverMessages();

    // Logs should converge
    try cluster.expectLogsConsistent();
}

test "test_log_divergence_many" {
    // Many divergent entries get reconciled
    var cluster = try RaftTestCluster.init(testing.allocator, 3);
    defer cluster.deinit();

    _ = try cluster.electLeader();
    const leader_id = cluster.findLeader();
    const follower_id: u64 = if (leader_id == 1) 2 else 1;

    // Add multiple conflicting entries to follower
    const follower = cluster.getNode(follower_id);
    var idx = follower.log.lastIndex() + 1;
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try follower.log.append(.{ .term = 99, .index = idx, .data = "diverge" });
        idx += 1;
    }

    // Leader syncs
    try cluster.heartbeat();
    try cluster.deliverMessages();
    try cluster.heartbeat();
    try cluster.deliverMessages();

    try cluster.expectLogsConsistent();
}

test "test_empty_log_follower_catches_up" {
    // A new node with an empty log receives the full log from the leader
    var cluster = try RaftTestCluster.init(testing.allocator, 3);
    defer cluster.deinit();

    // Isolate node 3 before anything happens
    try cluster.isolate(3);

    // Elect leader and submit entries
    _ = try cluster.electLeader();
    try cluster.submitAndCommit("full1");
    try cluster.submitAndCommit("full2");
    try cluster.submitAndCommit("full3");

    // Node 3 should have empty log
    const node3 = cluster.getNode(3);
    // It may have some entries from election messages, but committed entries are 0
    const ci_before = node3.commit_index;

    // Recover and sync
    cluster.recover();
    try cluster.heartbeat();
    try cluster.deliverMessages();
    try cluster.heartbeat();
    try cluster.deliverMessages();
    try cluster.heartbeat();
    try cluster.deliverMessages();

    // Node 3 should have caught up
    try testing.expect(node3.log.lastIndex() > 0);
    try testing.expect(node3.commit_index > ci_before);
}

test "test_log_compaction_truncate_before" {
    // Prefix truncation works correctly
    const alloc = testing.allocator;
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const dir = try std.fmt.allocPrint(alloc, "/tmp/zig_raft_logtest_{d}", .{timestamp});
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    defer wal.close();

    // Append entries 1..5
    try wal.append(.{ .term = 1, .index = 1, .data = "a" });
    try wal.append(.{ .term = 1, .index = 2, .data = "b" });
    try wal.append(.{ .term = 2, .index = 3, .data = "c" });
    try wal.append(.{ .term = 2, .index = 4, .data = "d" });
    try wal.append(.{ .term = 3, .index = 5, .data = "e" });

    // Truncate before index 3 (keep 3, 4, 5)
    try wal.truncateBefore(3);

    try testing.expectEqual(@as(u64, 3), wal.firstIndex());
    try testing.expectEqual(@as(u64, 5), wal.lastIndex());
    try testing.expectEqual(@as(usize, 3), wal.entryCount());

    const e3 = try wal.getEntry(3);
    try testing.expectEqualStrings("c", e3.data);
    try testing.expectEqual(@as(u64, 2), e3.term);
}

test "test_log_after_multiple_leader_changes" {
    // Log remains correct after several elections
    var cluster = try RaftTestCluster.init(testing.allocator, 3);
    defer cluster.deinit();

    // First leader
    _ = try cluster.electLeader();
    try cluster.submitAndCommit("term1_data");

    const first_leader = cluster.findLeader();

    // Force second election by isolating first leader
    try cluster.isolate(first_leader);
    const candidate2: u64 = if (first_leader == 1) 2 else 1;
    try cluster.triggerElection(candidate2);
    try cluster.deliverMessages();

    // Submit under new leader
    const second_leader = cluster.findLeader();
    if (second_leader != 0 and second_leader != first_leader) {
        const leader_node = cluster.getNode(second_leader);
        leader_node.submit("term2_data") catch {};
        const broadcast = leader_node.broadcastAppendEntries() catch &.{};
        for (broadcast) |msg| {
            cluster.pending.append(msg) catch {};
        }
        testing.allocator.free(broadcast);
        cluster.deliverMessages() catch {};
    }

    // Recover first leader
    cluster.recover();
    try cluster.heartbeat();
    try cluster.deliverMessages();

    // Logs should be consistent
    try cluster.expectLogsConsistent();
}

test "test_noop_committed_on_election" {
    // Leader's noop entry gets committed
    var cluster = try RaftTestCluster.init(testing.allocator, 3);
    defer cluster.deinit();

    _ = try cluster.electLeader();
    const leader_id = cluster.findLeader();
    const leader = cluster.getNode(leader_id);

    // The noop should be committed after election + delivery
    try testing.expect(leader.commit_index > 0);

    // The committed entry at the noop index should be of type noop
    const noop_entry = try leader.log.getEntry(1); // First entry is the noop
    try testing.expectEqual(raft_types.EntryType.noop, noop_entry.entry_type);
}
