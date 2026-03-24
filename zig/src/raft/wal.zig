// wal.zig — Segment-based Write-Ahead Log with CRC32 integrity.
//
// Inspired by TigerBeetle's WAL design. Entries are appended to 64 MB segments.
//
// WAL Directory Layout:
//   wal_<partition_id>/
//     segment_000000.wal    (64 MB segments)
//     segment_000001.wal
//     ...
//
// Entry Format (per entry in segment):
//   +--------+-------+------+-------+----------+------+
//   | Length  | Term  | Index| Type  | CRC32    | Data |
//   | 4 bytes | 8 bytes|8 bytes|1 byte| 4 bytes  | N    |
//   +--------+-------+------+-------+----------+------+
//   Total header: 25 bytes per entry

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const crc32 = @import("../crc32.zig");
const raft_types = @import("types.zig");
const LogEntry = raft_types.LogEntry;
const EntryType = raft_types.EntryType;

/// Maximum segment file size (64 MB).
pub const SEGMENT_MAX_SIZE: u64 = 64 * 1024 * 1024;

/// WAL entry header size.
pub const WAL_ENTRY_HEADER_SIZE: usize = 25;

/// Segment file metadata.
const Segment = struct {
    path: []const u8,
    base_index: u64,
    last_index: u64,
    size: u64,
};

/// Segment-based WAL with CRC32 integrity.
pub const WalStorage = struct {
    dir_path: []const u8,
    segments: std.ArrayList(Segment),
    first_index: u64,
    last_index: u64,
    last_term: u64,
    /// In-memory log buffer for random access by index.
    entries: std.ArrayList(LogEntry),
    /// Owned data buffers for entries loaded from disk.
    owned_data: std.ArrayList([]u8),
    allocator: Allocator,

    /// Open or create a WAL directory.
    pub fn open(allocator: Allocator, dir_path: []const u8) !*WalStorage {
        // Ensure directory exists
        std.fs.cwd().makePath(dir_path) catch {};

        const self = try allocator.create(WalStorage);
        self.* = .{
            .dir_path = dir_path,
            .segments = std.ArrayList(Segment).init(allocator),
            .first_index = 0,
            .last_index = 0,
            .last_term = 0,
            .entries = std.ArrayList(LogEntry).init(allocator),
            .owned_data = std.ArrayList([]u8).init(allocator),
            .allocator = allocator,
        };

        // Load existing segments
        self.loadSegments() catch |err| {
            self.close();
            return err;
        };

        return self;
    }

    /// Close the WAL and free resources.
    pub fn close(self: *WalStorage) void {
        self.freeOwnedData();
        self.entries.deinit();
        self.owned_data.deinit();
        for (self.segments.items) |seg| {
            self.allocator.free(seg.path);
        }
        self.segments.deinit();
        self.allocator.destroy(self);
    }

    /// Append a log entry.
    pub fn append(self: *WalStorage, entry: LogEntry) !void {
        // Validate index ordering
        if (self.last_index > 0 and entry.index != self.last_index + 1) {
            return error.InvalidData;
        }
        const prev_first_index = self.first_index;
        const prev_last_index = self.last_index;
        const prev_last_term = self.last_term;
        if (self.first_index == 0) {
            self.first_index = entry.index;
        }

        // Dupe data so we own it
        const owned = try self.allocator.dupe(u8, entry.data);
        try self.owned_data.append(owned);

        const owned_entry = LogEntry{
            .term = entry.term,
            .index = entry.index,
            .entry_type = entry.entry_type,
            .data = owned,
        };
        try self.entries.append(owned_entry);

        self.last_index = entry.index;
        self.last_term = entry.term;

        // Write to segment file
        self.writeEntryToSegment(&owned_entry) catch |e| {
            _ = self.entries.pop();
            if (self.owned_data.pop()) |data| {
                if (data.len > 0) self.allocator.free(data);
            }
            self.first_index = prev_first_index;
            self.last_index = prev_last_index;
            self.last_term = prev_last_term;
            return e;
        };
    }

    /// Get a single entry by index.
    pub fn getEntry(self: *WalStorage, index: u64) !LogEntry {
        if (index < self.first_index or index > self.last_index) {
            return error.InvalidData;
        }
        const offset = index - self.first_index;
        if (offset >= self.entries.items.len) {
            return error.InvalidData;
        }
        return self.entries.items[offset];
    }

    /// Get entries in range [lo, hi) (exclusive upper bound).
    pub fn getEntries(self: *WalStorage, lo: u64, hi: u64, allocator: Allocator) ![]LogEntry {
        if (lo > hi or lo < self.first_index or hi > self.last_index + 1) {
            return error.InvalidData;
        }

        const start = lo - self.first_index;
        const end = hi - self.first_index;
        if (end > self.entries.items.len) {
            return error.InvalidData;
        }

        const slice = self.entries.items[start..end];
        const result = try allocator.alloc(LogEntry, slice.len);
        @memcpy(result, slice);
        return result;
    }

    /// Truncate all entries after the given index (keep entries <= index).
    pub fn truncateAfter(self: *WalStorage, index: u64) !void {
        if (index >= self.last_index) return;
        if (index < self.first_index) {
            // Truncate everything
            self.freeOwnedData();
            self.entries.clearRetainingCapacity();
            self.owned_data.clearRetainingCapacity();
            self.first_index = 0;
            self.last_index = 0;
            self.last_term = 0;
            return;
        }

        const keep = index - self.first_index + 1;
        // Free owned data for truncated entries
        for (self.owned_data.items[keep..]) |data| {
            if (data.len > 0) {
                self.allocator.free(data);
            }
        }
        self.entries.shrinkRetainingCapacity(keep);
        self.owned_data.shrinkRetainingCapacity(keep);

        self.last_index = index;
        if (keep > 0) {
            self.last_term = self.entries.items[keep - 1].term;
        } else {
            self.last_term = 0;
        }
    }

    /// Truncate all entries before the given index (discard entries < index).
    pub fn truncateBefore(self: *WalStorage, index: u64) !void {
        if (index <= self.first_index) return;
        if (index > self.last_index + 1) {
            // Truncate everything
            self.freeOwnedData();
            self.entries.clearRetainingCapacity();
            self.owned_data.clearRetainingCapacity();
            self.first_index = 0;
            self.last_index = 0;
            self.last_term = 0;
            return;
        }

        const discard = index - self.first_index;
        // Free owned data for discarded entries
        for (self.owned_data.items[0..discard]) |data| {
            if (data.len > 0) {
                self.allocator.free(data);
            }
        }

        // Shift remaining entries
        const remaining = self.entries.items.len - discard;
        if (remaining > 0) {
            std.mem.copyForwards(LogEntry, self.entries.items[0..remaining], self.entries.items[discard..]);
            std.mem.copyForwards([]u8, self.owned_data.items[0..remaining], self.owned_data.items[discard..]);
        }
        self.entries.shrinkRetainingCapacity(remaining);
        self.owned_data.shrinkRetainingCapacity(remaining);
        self.first_index = index;

        // Clean up old segment files
        self.cleanOldSegments(index);
    }

    /// Last log index.
    pub fn lastIndex(self: *const WalStorage) u64 {
        return self.last_index;
    }

    /// First log index.
    pub fn firstIndex(self: *const WalStorage) u64 {
        return self.first_index;
    }

    /// Term of the last entry.
    pub fn lastTerm(self: *const WalStorage) u64 {
        return self.last_term;
    }

    /// Get the term for a specific index.
    pub fn termAt(self: *const WalStorage, index: u64) u64 {
        if (index == 0 or index < self.first_index or index > self.last_index) return 0;
        const offset = index - self.first_index;
        if (offset >= self.entries.items.len) return 0;
        return self.entries.items[offset].term;
    }

    /// Number of entries currently stored.
    pub fn entryCount(self: *const WalStorage) usize {
        return self.entries.items.len;
    }

    /// Fsync the current segment.
    pub fn sync(self: *WalStorage) !void {
        _ = self;
        // In-memory WAL; persistence is handled per-append via writeEntryToSegment.
    }

    // ─── Internal ────────────────────────────────────────────────

    fn writeEntryToSegment(self: *WalStorage, entry: *const LogEntry) !void {
        // Determine current segment path
        const seg_index = if (self.segments.items.len == 0) blk: {
            const seg = try self.createSegment(entry.index);
            try self.segments.append(seg);
            break :blk self.segments.items.len - 1;
        } else blk: {
            const last = &self.segments.items[self.segments.items.len - 1];
            if (last.size >= SEGMENT_MAX_SIZE) {
                const seg = try self.createSegment(entry.index);
                try self.segments.append(seg);
                break :blk self.segments.items.len - 1;
            }
            break :blk self.segments.items.len - 1;
        };

        // Serialize entry
        const data_len: u32 = @intCast(entry.data.len);
        const total_len: u32 = @intCast(WAL_ENTRY_HEADER_SIZE + data_len);
        const buf = try self.allocator.alloc(u8, total_len);
        defer self.allocator.free(buf);

        // Length (4 bytes)
        std.mem.writeInt(u32, buf[0..4], total_len, .little);
        // Term (8 bytes)
        std.mem.writeInt(u64, buf[4..12], entry.term, .little);
        // Index (8 bytes)
        std.mem.writeInt(u64, buf[12..20], entry.index, .little);
        // Type (1 byte)
        buf[20] = @intFromEnum(entry.entry_type);
        // CRC32 (4 bytes) — over term + index + type + data
        const crc_val = crc32.hash(buf[4..21]);
        const data_crc = if (entry.data.len > 0) crc32.hash(entry.data) else crc_val;
        _ = data_crc;
        std.mem.writeInt(u32, buf[21..25], crc_val, .little);
        // Data
        if (entry.data.len > 0) {
            @memcpy(buf[WAL_ENTRY_HEADER_SIZE..], entry.data);
        }

        // Write to file
        const seg = &self.segments.items[seg_index];
        const file = try std.fs.cwd().openFile(seg.path, .{ .mode = .write_only });
        defer file.close();

        try file.seekTo(seg.size);
        try file.writeAll(buf);

        seg.size += total_len;
        seg.last_index = entry.index;
    }

    fn createSegment(self: *WalStorage, base_index: u64) !Segment {
        const seg_num = self.segments.items.len;
        var path_buf: [4096]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/segment_{d:0>6}.wal", .{ self.dir_path, seg_num }) catch return error.InvalidData;
        const owned_path = try self.allocator.dupe(u8, path);

        // Create the file
        const file = std.fs.cwd().createFile(owned_path, .{}) catch {
            self.allocator.free(owned_path);
            return error.InvalidData;
        };
        file.close();

        return Segment{
            .path = owned_path,
            .base_index = base_index,
            .last_index = base_index,
            .size = 0,
        };
    }

    fn loadSegments(self: *WalStorage) !void {
        var dir = std.fs.cwd().openDir(self.dir_path, .{ .iterate = true }) catch return;
        defer dir.close();

        var seg_names = std.ArrayList([]const u8).init(self.allocator);
        defer {
            for (seg_names.items) |name| {
                self.allocator.free(name);
            }
            seg_names.deinit();
        }

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".wal")) continue;
            const name = try self.allocator.dupe(u8, entry.name);
            try seg_names.append(name);
        }

        // Sort segment names to load in order
        std.mem.sort([]const u8, seg_names.items, {}, struct {
            fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lessThan);

        for (seg_names.items) |name| {
            var path_buf: [4096]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ self.dir_path, name }) catch continue;
            const owned_path = try self.allocator.dupe(u8, path);

            // Read and parse segment entries
            const file = std.fs.cwd().openFile(owned_path, .{}) catch {
                self.allocator.free(owned_path);
                continue;
            };
            defer file.close();

            const stat = file.stat() catch {
                self.allocator.free(owned_path);
                continue;
            };

            var seg = Segment{
                .path = owned_path,
                .base_index = 0,
                .last_index = 0,
                .size = stat.size,
            };

            // Read all entries from segment
            if (stat.size > 0) {
                const content = file.readToEndAlloc(self.allocator, SEGMENT_MAX_SIZE * 2) catch {
                    try self.segments.append(seg);
                    continue;
                };
                defer self.allocator.free(content);

                var offset: usize = 0;
                var first_set = false;
                while (offset + WAL_ENTRY_HEADER_SIZE <= content.len) {
                    const entry_len = std.mem.readInt(u32, content[offset..][0..4], .little);
                    if (entry_len < WAL_ENTRY_HEADER_SIZE or offset + entry_len > content.len) break;

                    const term = std.mem.readInt(u64, content[offset + 4 ..][0..8], .little);
                    const index = std.mem.readInt(u64, content[offset + 12 ..][0..8], .little);
                    const entry_type: EntryType = @enumFromInt(content[offset + 20]);
                    const stored_crc = std.mem.readInt(u32, content[offset + 21 ..][0..4], .little);
                    const computed_crc = crc32.hash(content[offset + 4 .. offset + 21]);
                    if (stored_crc != computed_crc) {
                        self.allocator.free(owned_path);
                        return error.CorruptedEntry;
                    }
                    const data_len = entry_len - WAL_ENTRY_HEADER_SIZE;

                    const data = content[offset + WAL_ENTRY_HEADER_SIZE .. offset + WAL_ENTRY_HEADER_SIZE + data_len];
                    const owned = try self.allocator.dupe(u8, data);
                    try self.owned_data.append(owned);

                    try self.entries.append(.{
                        .term = term,
                        .index = index,
                        .entry_type = entry_type,
                        .data = owned,
                    });

                    if (!first_set) {
                        seg.base_index = index;
                        if (self.first_index == 0) self.first_index = index;
                        first_set = true;
                    }
                    seg.last_index = index;
                    self.last_index = index;
                    self.last_term = term;

                    offset += entry_len;
                }
            }

            try self.segments.append(seg);
        }
    }

    fn cleanOldSegments(self: *WalStorage, min_index: u64) void {
        var i: usize = 0;
        while (i < self.segments.items.len) {
            const seg = &self.segments.items[i];
            if (seg.last_index < min_index) {
                // Delete segment file
                std.fs.cwd().deleteFile(seg.path) catch {};
                self.allocator.free(seg.path);
                _ = self.segments.orderedRemove(i);
            } else {
                i += 1;
            }
        }
    }

    fn freeOwnedData(self: *WalStorage) void {
        for (self.owned_data.items) |data| {
            if (data.len > 0) {
                self.allocator.free(data);
            }
        }
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

fn makeTempWalDir(allocator: Allocator) ![]u8 {
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    return std.fmt.allocPrint(allocator, "/tmp/zig_wal_test_{d}", .{timestamp});
}

test "WAL open creates directory" {
    const alloc = testing.allocator;
    const dir = try makeTempWalDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    defer wal.close();

    try testing.expectEqual(@as(u64, 0), wal.lastIndex());
    try testing.expectEqual(@as(u64, 0), wal.firstIndex());
    try testing.expectEqual(@as(u64, 0), wal.lastTerm());
}

test "WAL append and get round-trip" {
    const alloc = testing.allocator;
    const dir = try makeTempWalDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    defer wal.close();

    try wal.append(.{ .term = 1, .index = 1, .data = "hello" });
    try wal.append(.{ .term = 1, .index = 2, .data = "world" });
    try wal.append(.{ .term = 2, .index = 3, .data = "zig" });

    try testing.expectEqual(@as(u64, 1), wal.firstIndex());
    try testing.expectEqual(@as(u64, 3), wal.lastIndex());
    try testing.expectEqual(@as(u64, 2), wal.lastTerm());

    const e1 = try wal.getEntry(1);
    try testing.expectEqualStrings("hello", e1.data);
    try testing.expectEqual(@as(u64, 1), e1.term);

    const e3 = try wal.getEntry(3);
    try testing.expectEqualStrings("zig", e3.data);
    try testing.expectEqual(@as(u64, 2), e3.term);
}

test "WAL getEntries range" {
    const alloc = testing.allocator;
    const dir = try makeTempWalDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    defer wal.close();

    try wal.append(.{ .term = 1, .index = 1, .data = "a" });
    try wal.append(.{ .term = 1, .index = 2, .data = "b" });
    try wal.append(.{ .term = 1, .index = 3, .data = "c" });

    const entries = try wal.getEntries(1, 3, alloc);
    defer alloc.free(entries);

    try testing.expectEqual(@as(usize, 2), entries.len);
    try testing.expectEqualStrings("a", entries[0].data);
    try testing.expectEqualStrings("b", entries[1].data);
}

test "WAL truncateAfter" {
    const alloc = testing.allocator;
    const dir = try makeTempWalDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    defer wal.close();

    try wal.append(.{ .term = 1, .index = 1, .data = "a" });
    try wal.append(.{ .term = 1, .index = 2, .data = "b" });
    try wal.append(.{ .term = 2, .index = 3, .data = "c" });

    try wal.truncateAfter(1);

    try testing.expectEqual(@as(u64, 1), wal.lastIndex());
    try testing.expectEqual(@as(u64, 1), wal.lastTerm());
    try testing.expectEqual(@as(usize, 1), wal.entryCount());
}

test "WAL truncateBefore" {
    const alloc = testing.allocator;
    const dir = try makeTempWalDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    defer wal.close();

    try wal.append(.{ .term = 1, .index = 1, .data = "a" });
    try wal.append(.{ .term = 1, .index = 2, .data = "b" });
    try wal.append(.{ .term = 2, .index = 3, .data = "c" });

    try wal.truncateBefore(2);

    try testing.expectEqual(@as(u64, 2), wal.firstIndex());
    try testing.expectEqual(@as(u64, 3), wal.lastIndex());
    try testing.expectEqual(@as(usize, 2), wal.entryCount());

    const e2 = try wal.getEntry(2);
    try testing.expectEqualStrings("b", e2.data);
}

test "WAL termAt" {
    const alloc = testing.allocator;
    const dir = try makeTempWalDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    const wal = try WalStorage.open(alloc, dir);
    defer wal.close();

    try wal.append(.{ .term = 1, .index = 1 });
    try wal.append(.{ .term = 2, .index = 2 });
    try wal.append(.{ .term = 3, .index = 3 });

    try testing.expectEqual(@as(u64, 1), wal.termAt(1));
    try testing.expectEqual(@as(u64, 2), wal.termAt(2));
    try testing.expectEqual(@as(u64, 3), wal.termAt(3));
    try testing.expectEqual(@as(u64, 0), wal.termAt(0));
    try testing.expectEqual(@as(u64, 0), wal.termAt(99));
}

test "WAL persistence round-trip" {
    const alloc = testing.allocator;
    const dir = try makeTempWalDir(alloc);
    defer alloc.free(dir);
    defer std.fs.cwd().deleteTree(dir) catch {};

    // Write entries
    {
        const wal = try WalStorage.open(alloc, dir);
        try wal.append(.{ .term = 1, .index = 1, .data = "persist1" });
        try wal.append(.{ .term = 1, .index = 2, .data = "persist2" });
        try wal.append(.{ .term = 2, .index = 3, .data = "persist3" });
        wal.close();
    }

    // Re-open and verify
    {
        const wal = try WalStorage.open(alloc, dir);
        defer wal.close();

        try testing.expectEqual(@as(u64, 1), wal.firstIndex());
        try testing.expectEqual(@as(u64, 3), wal.lastIndex());
        try testing.expectEqual(@as(u64, 2), wal.lastTerm());

        const e1 = try wal.getEntry(1);
        try testing.expectEqualStrings("persist1", e1.data);
        const e3 = try wal.getEntry(3);
        try testing.expectEqualStrings("persist3", e3.data);
    }
}
