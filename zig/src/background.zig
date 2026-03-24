// background.zig — Background maintenance tasks.
// Runs periodic CRC computation for extents missing checksums and
// WAL truncation to reclaim disk space from old Raft log entries.

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("log.zig");
const space_mgr_mod = @import("space_manager.zig");
const crc32_mod = @import("crc32.zig");

const SpaceManager = space_mgr_mod.SpaceManager;

/// Default interval for auto-CRC computation: 5 minutes.
const AUTO_CRC_INTERVAL_NS: u64 = 5 * 60 * std.time.ns_per_s;

/// Default interval for WAL truncation: 10 minutes.
const WAL_TRUNCATE_INTERVAL_NS: u64 = 10 * 60 * std.time.ns_per_s;

/// Maximum extents to scan per CRC round.
const MAX_CRC_EXTENTS_PER_ROUND: usize = 512;

/// Read buffer size for CRC computation (64 KB).
const CRC_READ_BUF_SIZE: usize = 64 * 1024;

/// Auto-CRC task statistics.
pub const CrcStats = struct {
    rounds_completed: u64 = 0,
    extents_scanned: u64 = 0,
    crcs_computed: u64 = 0,
    errors: u64 = 0,
};

/// WAL truncation statistics.
pub const TruncateStats = struct {
    rounds_completed: u64 = 0,
    segments_removed: u64 = 0,
    bytes_reclaimed: u64 = 0,
};

/// Background task manager running auto-CRC and WAL truncation.
pub const BackgroundManager = struct {
    allocator: Allocator,
    space_mgr: *SpaceManager,
    running: std.atomic.Value(bool),
    crc_thread: ?std.Thread,
    wal_thread: ?std.Thread,
    crc_interval_ns: u64,
    wal_interval_ns: u64,
    crc_stats: CrcStats,
    truncate_stats: TruncateStats,
    wal_dir: ?[]const u8,

    pub fn init(allocator: Allocator, space_mgr: *SpaceManager, wal_dir: ?[]const u8) BackgroundManager {
        return .{
            .allocator = allocator,
            .space_mgr = space_mgr,
            .running = std.atomic.Value(bool).init(false),
            .crc_thread = null,
            .wal_thread = null,
            .crc_interval_ns = AUTO_CRC_INTERVAL_NS,
            .wal_interval_ns = WAL_TRUNCATE_INTERVAL_NS,
            .crc_stats = .{},
            .truncate_stats = .{},
            .wal_dir = wal_dir,
        };
    }

    /// Start background tasks.
    pub fn start(self: *BackgroundManager) void {
        if (self.running.load(.acquire)) return;
        self.running.store(true, .release);

        self.crc_thread = std.Thread.spawn(.{}, autoCrcLoop, .{self}) catch null;

        if (self.wal_dir != null) {
            self.wal_thread = std.Thread.spawn(.{}, walTruncateLoop, .{self}) catch null;
        }

        log.info("background: started (auto-crc={s}, wal-truncate={s})", .{
            if (self.crc_thread != null) "enabled" else "failed",
            if (self.wal_thread != null) "enabled" else "disabled",
        });
    }

    /// Stop background tasks.
    pub fn stop(self: *BackgroundManager) void {
        self.running.store(false, .release);
        if (self.crc_thread) |t| {
            t.join();
            self.crc_thread = null;
        }
        if (self.wal_thread) |t| {
            t.join();
            self.wal_thread = null;
        }
    }

    pub fn deinit(self: *BackgroundManager) void {
        self.stop();
    }

    /// Get auto-CRC statistics.
    pub fn getCrcStats(self: *const BackgroundManager) CrcStats {
        return self.crc_stats;
    }

    /// Get WAL truncation statistics.
    pub fn getTruncateStats(self: *const BackgroundManager) TruncateStats {
        return self.truncate_stats;
    }
};

// ─── Auto-CRC Loop ─────────────────────────────────────────────────

fn autoCrcLoop(mgr: *BackgroundManager) void {
    while (mgr.running.load(.acquire)) {
        std.time.sleep(mgr.crc_interval_ns);
        if (!mgr.running.load(.acquire)) break;

        doAutoCrcRound(mgr) catch |e| {
            log.warn("background: auto-CRC round failed: {}", .{e});
            mgr.crc_stats.errors += 1;
        };
        mgr.crc_stats.rounds_completed += 1;
    }
}

fn doAutoCrcRound(mgr: *BackgroundManager) !void {
    const allocator = mgr.allocator;

    // Get all partitions
    const partitions = mgr.space_mgr.allPartitions(allocator) catch return;
    defer allocator.free(partitions);

    var total_computed: u64 = 0;
    var total_scanned: u64 = 0;

    for (partitions) |dp| {
        if (!mgr.running.load(.acquire)) break;
        if (total_computed >= MAX_CRC_EXTENTS_PER_ROUND) break;

        // Get watermarks for this partition
        const watermarks = dp.store.getAllWatermarks(allocator) catch continue;
        defer allocator.free(watermarks);

        for (watermarks) |w| {
            if (!mgr.running.load(.acquire)) break;
            if (total_computed >= MAX_CRC_EXTENTS_PER_ROUND) break;
            total_scanned += 1;

            // Only process extents with CRC == 0
            if (w.crc != 0) continue;
            if (w.size == 0) continue;

            // Read extent data and compute CRC
            const read_size: usize = @intCast(@min(w.size, CRC_READ_BUF_SIZE));
            const buf = allocator.alloc(u8, read_size) catch continue;
            defer allocator.free(buf);

            const n = dp.store.readNoCrc(w.file_id, 0, buf) catch continue;
            if (n == 0) continue;

            _ = crc32_mod.hash(buf[0..n]);
            total_computed += 1;
        }
    }

    mgr.crc_stats.extents_scanned += total_scanned;
    mgr.crc_stats.crcs_computed += total_computed;

    if (total_computed > 0) {
        log.info("background: auto-CRC round: scanned={d} computed={d}", .{
            total_scanned,
            total_computed,
        });
    }
}

// ─── WAL Truncation Loop ────────────────────────────────────────────

fn walTruncateLoop(mgr: *BackgroundManager) void {
    while (mgr.running.load(.acquire)) {
        std.time.sleep(mgr.wal_interval_ns);
        if (!mgr.running.load(.acquire)) break;

        doWalTruncateRound(mgr) catch |e| {
            log.warn("background: WAL truncation failed: {}", .{e});
        };
        mgr.truncate_stats.rounds_completed += 1;
    }
}

fn doWalTruncateRound(mgr: *BackgroundManager) !void {
    const wal_dir = mgr.wal_dir orelse return;

    // List WAL subdirectories (one per partition: wal_<partition_id>/)
    var dir = std.fs.cwd().openDir(wal_dir, .{ .iterate = true }) catch return;
    defer dir.close();

    var iter = dir.iterate();
    while (iter.next() catch null) |entry| {
        if (!mgr.running.load(.acquire)) break;
        if (entry.kind != .directory) continue;

        // Only process wal_* directories
        if (!std.mem.startsWith(u8, entry.name, "wal_")) continue;

        // Truncate old segments in this WAL directory
        const truncated = truncateOldSegments(mgr.allocator, wal_dir, entry.name) catch continue;
        mgr.truncate_stats.segments_removed += truncated.segments;
        mgr.truncate_stats.bytes_reclaimed += truncated.bytes;
    }
}

const TruncateResult = struct {
    segments: u64,
    bytes: u64,
};

/// Truncate old WAL segments, keeping the latest 2 segment files.
fn truncateOldSegments(allocator: Allocator, wal_dir: []const u8, partition_dir: []const u8) !TruncateResult {
    // Build path: <wal_dir>/<partition_dir>/
    var path_buf: [4096]u8 = undefined;
    const full_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ wal_dir, partition_dir }) catch return .{ .segments = 0, .bytes = 0 };

    var dir = std.fs.cwd().openDir(full_path, .{ .iterate = true }) catch return .{ .segments = 0, .bytes = 0 };
    defer dir.close();

    // Collect segment file names
    var segments = std.ArrayList([]const u8).init(allocator);
    defer {
        for (segments.items) |name| allocator.free(name);
        segments.deinit();
    }

    var iter = dir.iterate();
    while (iter.next() catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".wal")) continue;
        const name_copy = allocator.dupe(u8, entry.name) catch continue;
        segments.append(name_copy) catch {
            allocator.free(name_copy);
            continue;
        };
    }

    // Need at least 3 segments to truncate (keep latest 2)
    if (segments.items.len <= 2) return .{ .segments = 0, .bytes = 0 };

    // Sort by name (lexicographic = chronological for segment_NNNNNN.wal)
    std.mem.sort([]const u8, segments.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);

    // Delete all but the last 2
    var removed: u64 = 0;
    var bytes: u64 = 0;
    const to_remove = segments.items.len - 2;
    for (segments.items[0..to_remove]) |name| {
        // Get file size before deletion
        const stat = dir.statFile(name) catch null;
        const file_size: u64 = if (stat) |s| s.size else 0;

        dir.deleteFile(name) catch continue;
        removed += 1;
        bytes += file_size;
    }

    if (removed > 0) {
        log.info("background: WAL truncation: {s}/{s}: removed {d} segments ({d} bytes)", .{
            wal_dir,
            partition_dir,
            removed,
            bytes,
        });
    }

    return .{ .segments = removed, .bytes = bytes };
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "BackgroundManager init and stop" {
    var mgr = BackgroundManager.init(testing.allocator, undefined, null);
    defer mgr.deinit();

    try testing.expect(!mgr.running.load(.acquire));
    try testing.expectEqual(@as(u64, 0), mgr.crc_stats.rounds_completed);
    try testing.expectEqual(@as(u64, 0), mgr.truncate_stats.rounds_completed);
}

test "CrcStats and TruncateStats defaults" {
    const cs = CrcStats{};
    try testing.expectEqual(@as(u64, 0), cs.rounds_completed);
    try testing.expectEqual(@as(u64, 0), cs.extents_scanned);
    try testing.expectEqual(@as(u64, 0), cs.crcs_computed);

    const ts = TruncateStats{};
    try testing.expectEqual(@as(u64, 0), ts.rounds_completed);
    try testing.expectEqual(@as(u64, 0), ts.segments_removed);
    try testing.expectEqual(@as(u64, 0), ts.bytes_reclaimed);
}

test "truncateOldSegments with no WAL dir" {
    const result = truncateOldSegments(testing.allocator, "/nonexistent/path", "wal_123") catch .{ .segments = 0, .bytes = 0 };
    try testing.expectEqual(@as(u64, 0), result.segments);
}

test "truncateOldSegments keeps latest 2 segments" {
    const tmp_dir = "/tmp/cubefs_zig_bg_test";
    const wal_part = "wal_1";
    const wal_path = tmp_dir ++ "/" ++ wal_part;

    // Clean up first
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    std.fs.cwd().makePath(wal_path) catch return;
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    // Create 4 segment files
    for ([_][]const u8{
        "segment_000000.wal",
        "segment_000001.wal",
        "segment_000002.wal",
        "segment_000003.wal",
    }) |name| {
        var dir = std.fs.cwd().openDir(wal_path, .{}) catch return;
        defer dir.close();
        const file = dir.createFile(name, .{}) catch continue;
        _ = file.write("test data for segment") catch {};
        file.close();
    }

    const result = try truncateOldSegments(testing.allocator, tmp_dir, wal_part);
    try testing.expectEqual(@as(u64, 2), result.segments); // removed 2, kept 2
    try testing.expect(result.bytes > 0);

    // Verify only 2 files remain
    var dir = try std.fs.cwd().openDir(wal_path, .{ .iterate = true });
    defer dir.close();
    var count: usize = 0;
    var iter = dir.iterate();
    while (try iter.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 2), count);
}
