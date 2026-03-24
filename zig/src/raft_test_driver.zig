// raft_test_driver.zig — Aggregates all src/raft/*.zig tests.
// This file lives in src/ so that src/raft/ files can import ../raft.zig etc.

comptime {
    _ = @import("raft/types.zig");
    _ = @import("raft/messages.zig");
    _ = @import("raft/wal.zig");
    _ = @import("raft/core.zig");
    _ = @import("raft/transport.zig");
    _ = @import("raft/raft_instance.zig");
    _ = @import("raft/server.zig");
    _ = @import("raft/test_cluster.zig");
    _ = @import("raft/test_paper.zig");
    _ = @import("raft/test_failure.zig");
    _ = @import("raft/test_log.zig");
    _ = @import("raft/test_wal.zig");
}
