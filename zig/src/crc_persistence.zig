// crc_persistence.zig — Persist per-block CRCs to an EXTENT_CRC file.
// Matches Go's storage/extent_store_crc.go format.
//
// File layout:
//   Each extent gets 4096 bytes at offset `extent_id * 4096`.
//   Each 128KB block stores a 4-byte CRC at `(extent_id * 4096) + (block_no * 4)`.
//   Max blocks per extent = 4096 / 4 = 1024 (covering 1024 * 128KB = 128MB).

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const log = @import("log.zig");

/// Size of the CRC region per extent in the CRC file.
const CRC_REGION_SIZE: u64 = 4096;

/// Number of CRC slots per extent (4096 / 4 = 1024 blocks).
pub const MAX_BLOCKS_PER_EXTENT: u32 = 1024;

/// Block size for CRC granularity (128 KB, matching Go).
pub const CRC_BLOCK_SIZE: u64 = 128 * 1024;

/// Filename for the CRC persistence file.
pub const CRC_FILENAME: [:0]const u8 = "EXTENT_CRC";

pub const CrcPersistence = struct {
    fd: posix.fd_t,
    allocator: Allocator,

    /// Open (or create) the EXTENT_CRC file in the given data directory.
    pub fn init(allocator: Allocator, data_path: [:0]const u8) !CrcPersistence {
        const S = struct {
            threadlocal var buf: [4096]u8 = undefined;
        };
        const path = std.fmt.bufPrintZ(&S.buf, "{s}/{s}", .{ data_path, CRC_FILENAME }) catch {
            return error.InvalidArgument;
        };

        const fd = posix.open(path, .{
            .ACCMODE = .RDWR,
            .CREAT = true,
        }, 0o644) catch {
            return error.AccessDenied;
        };

        return CrcPersistence{
            .fd = fd,
            .allocator = allocator,
        };
    }

    /// Close the CRC file.
    pub fn deinit(self: *CrcPersistence) void {
        posix.close(self.fd);
    }

    /// Persist a single block CRC for an extent.
    pub fn persistBlockCrc(self: *CrcPersistence, extent_id: u64, block_no: u32, crc: u32) !void {
        if (block_no >= MAX_BLOCKS_PER_EXTENT) return;

        const file_offset = extent_id * CRC_REGION_SIZE + @as(u64, block_no) * 4;
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, crc, .little);

        _ = posix.pwrite(self.fd, &buf, file_offset) catch {
            return error.InputOutput;
        };
    }

    /// Persist multiple block CRCs for an extent at once.
    pub fn persistBlockCrcs(self: *CrcPersistence, extent_id: u64, crcs: []const u32) !void {
        if (crcs.len == 0) return;

        const count = @min(crcs.len, MAX_BLOCKS_PER_EXTENT);
        const file_offset = extent_id * CRC_REGION_SIZE;
        const byte_len = count * 4;
        var buf: [MAX_BLOCKS_PER_EXTENT * 4]u8 = undefined;

        for (0..count) |i| {
            const off = i * 4;
            std.mem.writeInt(u32, buf[off..][0..4], crcs[i], .little);
        }

        _ = posix.pwrite(self.fd, buf[0..byte_len], file_offset) catch {
            return error.InputOutput;
        };
    }

    /// Delete CRC data for an extent by zeroing its region.
    pub fn deleteBlockCrc(self: *CrcPersistence, extent_id: u64) !void {
        const file_offset = extent_id * CRC_REGION_SIZE;
        var zeros: [CRC_REGION_SIZE]u8 = std.mem.zeroes([CRC_REGION_SIZE]u8);

        _ = posix.pwrite(self.fd, &zeros, file_offset) catch {
            return error.InputOutput;
        };
    }

    /// Read all block CRCs for an extent.
    pub fn readBlockCrcs(self: *CrcPersistence, extent_id: u64) ![MAX_BLOCKS_PER_EXTENT]u32 {
        const file_offset = extent_id * CRC_REGION_SIZE;
        var buf: [CRC_REGION_SIZE]u8 = undefined;

        const n = posix.pread(self.fd, &buf, file_offset) catch {
            return std.mem.zeroes([MAX_BLOCKS_PER_EXTENT]u32);
        };

        var crcs = std.mem.zeroes([MAX_BLOCKS_PER_EXTENT]u32);
        const blocks_read = @min(n / 4, MAX_BLOCKS_PER_EXTENT);
        for (0..blocks_read) |i| {
            const off = i * 4;
            crcs[i] = std.mem.readInt(u32, buf[off..][0..4], .little);
        }

        return crcs;
    }

    /// Sync the CRC file to disk.
    pub fn sync(self: *CrcPersistence) !void {
        posix.fsync(self.fd) catch {
            return error.InputOutput;
        };
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "CrcPersistence persist and read round-trip" {
    // Create temp directory
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    var cp = CrcPersistence.init(testing.allocator, data_path) catch return;
    defer cp.deinit();

    // Write CRCs for extent 1
    try cp.persistBlockCrc(1, 0, 0xAABBCCDD);
    try cp.persistBlockCrc(1, 1, 0x11223344);

    // Read them back
    const crcs = try cp.readBlockCrcs(1);
    try testing.expectEqual(@as(u32, 0xAABBCCDD), crcs[0]);
    try testing.expectEqual(@as(u32, 0x11223344), crcs[1]);
    try testing.expectEqual(@as(u32, 0), crcs[2]); // unwritten slot is zero
}

test "CrcPersistence deleteBlockCrc zeros region" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    var cp = CrcPersistence.init(testing.allocator, data_path) catch return;
    defer cp.deinit();

    // Write and verify
    try cp.persistBlockCrc(5, 0, 0xDEADBEEF);
    var crcs = try cp.readBlockCrcs(5);
    try testing.expectEqual(@as(u32, 0xDEADBEEF), crcs[0]);

    // Delete and verify zeros
    try cp.deleteBlockCrc(5);
    crcs = try cp.readBlockCrcs(5);
    try testing.expectEqual(@as(u32, 0), crcs[0]);
}

test "CrcPersistence persistBlockCrcs batch write" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    var cp = CrcPersistence.init(testing.allocator, data_path) catch return;
    defer cp.deinit();

    const batch = [_]u32{ 0x11, 0x22, 0x33, 0x44 };
    try cp.persistBlockCrcs(10, &batch);

    const crcs = try cp.readBlockCrcs(10);
    try testing.expectEqual(@as(u32, 0x11), crcs[0]);
    try testing.expectEqual(@as(u32, 0x22), crcs[1]);
    try testing.expectEqual(@as(u32, 0x33), crcs[2]);
    try testing.expectEqual(@as(u32, 0x44), crcs[3]);
}

test "CrcPersistence ignores out-of-range block_no" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var path_buf: [4096]u8 = undefined;
    const tmp_path = tmp_dir.dir.realpath(".", &path_buf) catch return;
    path_buf[tmp_path.len] = 0;
    const data_path: [:0]const u8 = path_buf[0..tmp_path.len :0];

    var cp = CrcPersistence.init(testing.allocator, data_path) catch return;
    defer cp.deinit();

    // Should silently ignore block_no >= MAX_BLOCKS_PER_EXTENT
    try cp.persistBlockCrc(1, MAX_BLOCKS_PER_EXTENT, 0xFF);
    try cp.persistBlockCrc(1, MAX_BLOCKS_PER_EXTENT + 100, 0xFF);
}
