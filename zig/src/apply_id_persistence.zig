// apply_id_persistence.zig — Persist the last applied Raft ID to disk.
// Stores as ASCII u64 in <data_path>/APPLY file.

const std = @import("std");
const posix = std.posix;

/// Filename for apply ID persistence.
pub const APPLY_FILENAME: [:0]const u8 = "APPLY";

/// Load the persisted apply ID. Returns 0 if file doesn't exist or is invalid.
pub fn load(data_path: [:0]const u8) u64 {
    const S = struct {
        threadlocal var buf: [4096]u8 = undefined;
    };
    const path = std.fmt.bufPrintZ(&S.buf, "{s}/{s}", .{ data_path, APPLY_FILENAME }) catch return 0;

    const file = std.fs.cwd().openFile(path, .{}) catch return 0;
    defer file.close();

    var content_buf: [32]u8 = undefined;
    const n = file.readAll(&content_buf) catch return 0;
    if (n == 0) return 0;

    // Trim whitespace/newlines
    const trimmed = std.mem.trim(u8, content_buf[0..n], " \t\r\n");
    return std.fmt.parseInt(u64, trimmed, 10) catch 0;
}

/// Persist the apply ID to disk. Writes atomically (write + sync).
pub fn persist(data_path: [:0]const u8, apply_id: u64) !void {
    const S = struct {
        threadlocal var buf: [4096]u8 = undefined;
    };
    const path = std.fmt.bufPrintZ(&S.buf, "{s}/{s}", .{ data_path, APPLY_FILENAME }) catch {
        return error.InvalidArgument;
    };

    const file = std.fs.cwd().createFile(path, .{ .truncate = true }) catch {
        return error.AccessDenied;
    };
    defer file.close();

    var num_buf: [20]u8 = undefined;
    const formatted = std.fmt.bufPrint(&num_buf, "{d}", .{apply_id}) catch return error.InvalidArgument;
    file.writeAll(formatted) catch return error.InputOutput;
    file.sync() catch return error.InputOutput;
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "persist and load round-trip" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    // Initially 0
    try testing.expectEqual(@as(u64, 0), load(data_path));

    // Persist and load
    try persist(data_path, 42);
    try testing.expectEqual(@as(u64, 42), load(data_path));

    // Update
    try persist(data_path, 99999);
    try testing.expectEqual(@as(u64, 99999), load(data_path));
}

test "load returns 0 for nonexistent directory" {
    try testing.expectEqual(@as(u64, 0), load("/nonexistent/path/12345"));
}

test "persist large apply ID" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    const large_id: u64 = 18446744073709551615; // max u64
    try persist(data_path, large_id);
    try testing.expectEqual(large_id, load(data_path));
}
