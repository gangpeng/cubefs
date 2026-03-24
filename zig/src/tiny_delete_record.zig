// tiny_delete_record.zig — Persist tiny extent deletion records.
// Matches Go's storage/tiny_extent_delete_record.go format.
//
// File: <data_path>/TINYEXTENT_DELETE
// Format: Append-only file, each record is 24 bytes:
//   extent_id (u64 big-endian) + offset (u64 big-endian) + size (u64 big-endian)

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

/// Filename for tiny delete records.
pub const TINY_DELETE_FILENAME: [:0]const u8 = "TINYEXTENT_DELETE";

/// Size of a single delete record in bytes.
const RECORD_SIZE: usize = 24;

/// A single tiny extent deletion record.
pub const TinyDeleteRecord = struct {
    extent_id: u64,
    offset: u64,
    size: u64,
};

pub const TinyDeleteRecorder = struct {
    fd: posix.fd_t,

    /// Open (or create) the TINYEXTENT_DELETE file in the given data directory.
    pub fn init(data_path: [:0]const u8) !TinyDeleteRecorder {
        const S = struct {
            threadlocal var buf: [4096]u8 = undefined;
        };
        const path = std.fmt.bufPrintZ(&S.buf, "{s}/{s}", .{ data_path, TINY_DELETE_FILENAME }) catch {
            return error.InvalidArgument;
        };

        const fd = posix.open(path, .{
            .ACCMODE = .RDWR,
            .CREAT = true,
            .APPEND = true,
        }, 0o644) catch {
            return error.AccessDenied;
        };

        return TinyDeleteRecorder{ .fd = fd };
    }

    /// Close the delete record file.
    pub fn deinit(self: *TinyDeleteRecorder) void {
        posix.close(self.fd);
    }

    /// Append a deletion record.
    pub fn record(self: *TinyDeleteRecorder, extent_id: u64, offset: u64, size: u64) !void {
        var buf: [RECORD_SIZE]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], extent_id, .big);
        std.mem.writeInt(u64, buf[8..16], offset, .big);
        std.mem.writeInt(u64, buf[16..24], size, .big);

        _ = posix.write(self.fd, &buf) catch {
            return error.InputOutput;
        };
    }

    /// Read all deletion records from the file.
    pub fn readAll(self: *TinyDeleteRecorder, allocator: Allocator) ![]TinyDeleteRecord {
        // Get file size
        const stat = posix.fstat(self.fd) catch return error.InputOutput;
        const file_size: u64 = @intCast(stat.size);
        if (file_size < RECORD_SIZE) {
            return &.{};
        }

        const record_count = file_size / RECORD_SIZE;
        const read_size = record_count * RECORD_SIZE;

        const buf = try allocator.alloc(u8, read_size);
        defer allocator.free(buf);

        // Read from beginning
        var total_read: usize = 0;
        while (total_read < read_size) {
            const n = posix.pread(self.fd, buf[total_read..read_size], total_read) catch {
                return error.InputOutput;
            };
            if (n == 0) break;
            total_read += n;
        }

        const actual_records = total_read / RECORD_SIZE;
        const records = try allocator.alloc(TinyDeleteRecord, actual_records);

        for (0..actual_records) |i| {
            const base = i * RECORD_SIZE;
            records[i] = .{
                .extent_id = std.mem.readInt(u64, buf[base..][0..8], .big),
                .offset = std.mem.readInt(u64, buf[base + 8 ..][0..8], .big),
                .size = std.mem.readInt(u64, buf[base + 16 ..][0..8], .big),
            };
        }

        return records;
    }

    /// Sync the file to disk.
    pub fn sync(self: *TinyDeleteRecorder) !void {
        posix.fsync(self.fd) catch {
            return error.InputOutput;
        };
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "TinyDeleteRecorder record and readAll round-trip" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    var recorder = TinyDeleteRecorder.init(data_path) catch return;
    defer recorder.deinit();

    try recorder.record(1, 4096, 1024);
    try recorder.record(2, 8192, 2048);
    try recorder.record(3, 0, 512);

    const records = try recorder.readAll(testing.allocator);
    defer testing.allocator.free(records);

    try testing.expectEqual(@as(usize, 3), records.len);

    try testing.expectEqual(@as(u64, 1), records[0].extent_id);
    try testing.expectEqual(@as(u64, 4096), records[0].offset);
    try testing.expectEqual(@as(u64, 1024), records[0].size);

    try testing.expectEqual(@as(u64, 2), records[1].extent_id);
    try testing.expectEqual(@as(u64, 8192), records[1].offset);
    try testing.expectEqual(@as(u64, 2048), records[1].size);

    try testing.expectEqual(@as(u64, 3), records[2].extent_id);
    try testing.expectEqual(@as(u64, 0), records[2].offset);
    try testing.expectEqual(@as(u64, 512), records[2].size);
}

test "TinyDeleteRecorder readAll on empty file returns empty" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    var recorder = TinyDeleteRecorder.init(data_path) catch return;
    defer recorder.deinit();

    const records = try recorder.readAll(testing.allocator);
    try testing.expectEqual(@as(usize, 0), records.len);
}

test "TinyDeleteRecorder big-endian encoding" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    var recorder = TinyDeleteRecorder.init(data_path) catch return;
    defer recorder.deinit();

    try recorder.record(0x0102030405060708, 0x1112131415161718, 0x2122232425262728);

    const records = try recorder.readAll(testing.allocator);
    defer testing.allocator.free(records);

    try testing.expectEqual(@as(u64, 0x0102030405060708), records[0].extent_id);
    try testing.expectEqual(@as(u64, 0x1112131415161718), records[0].offset);
    try testing.expectEqual(@as(u64, 0x2122232425262728), records[0].size);
}
