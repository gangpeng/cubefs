// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Single extent I/O — mirrors Go `storage.Extent` / Rust `extent.rs`.
//!
//! Each extent is a file on disk with a CRC header tracking per-block checksums.
//! Performance optimizations:
//! - Separate read FD for lock-free pread (no mutex)
//! - Write FD protected by Mutex for serialized writes
//! - Atomic data_size / modify_time / dirty flag

const std = @import("std");
const posix = std.posix;
const constants = @import("constants.zig");
const err = @import("error.zig");
const types = @import("types.zig");
const crc32 = @import("crc32.zig");

const Allocator = std.mem.Allocator;
const Mutex = std.Thread.Mutex;
const RwLock = std.Thread.RwLock;

pub const Extent = struct {
    /// Write file descriptor (mutex-protected for serialized writes).
    write_fd: posix.fd_t,
    /// Read file descriptor (lock-free pread).
    read_fd: posix.fd_t,
    /// Path to the extent file on disk.
    file_path: []const u8,
    /// Extent ID.
    extent_id: u64,
    /// Last modification time (unix timestamp).
    modify_time: std.atomic.Value(i64),
    /// Current data size in bytes.
    data_size: std.atomic.Value(i64),
    /// Close status: -1 if closed, 0 if open.
    has_close: std.atomic.Value(i32),
    /// Per-block CRC header data.
    header: []u8,
    header_len: usize,
    header_capacity: usize,
    /// Whether the extent has unflushed writes.
    dirty: std.atomic.Value(bool),
    /// I/O operation counter for coarse-grained timestamp updates.
    io_count: std.atomic.Value(u64),
    /// Mutex for write serialization.
    write_mutex: Mutex,
    /// RwLock for header access.
    header_lock: RwLock,
    /// Allocator used for header buffer.
    allocator: Allocator,

    /// Create a new extent file at the given path.
    pub fn create(allocator: Allocator, path: []const u8, extent_id: u64) err.Error!*Extent {
        // Create the file (O_CREAT | O_EXCL | O_RDWR)
        const write_fd = posix.open(path, .{
            .ACCMODE = .RDWR,
            .CREAT = true,
            .EXCL = true,
        }, 0o644) catch return error.IoError;

        // Open a separate read-only FD
        const read_fd = posix.open(path, .{
            .ACCMODE = .RDONLY,
        }, 0) catch {
            posix.close(write_fd);
            return error.IoError;
        };

        const now = @as(i64, @intCast(std.time.timestamp()));

        const self = allocator.create(Extent) catch return error.IoError;
        self.* = Extent{
            .write_fd = write_fd,
            .read_fd = read_fd,
            .file_path = path,
            .extent_id = extent_id,
            .modify_time = std.atomic.Value(i64).init(now),
            .data_size = std.atomic.Value(i64).init(0),
            .has_close = std.atomic.Value(i32).init(0),
            .header = &.{},
            .header_len = 0,
            .header_capacity = 0,
            .dirty = std.atomic.Value(bool).init(false),
            .io_count = std.atomic.Value(u64).init(0),
            .write_mutex = Mutex{},
            .header_lock = RwLock{},
            .allocator = allocator,
        };
        return self;
    }

    /// Open an existing extent file.
    pub fn open(allocator: Allocator, path: []const u8, extent_id: u64) err.Error!*Extent {
        const write_fd = posix.open(path, .{
            .ACCMODE = .RDWR,
        }, 0) catch return error.IoError;

        const read_fd = posix.open(path, .{
            .ACCMODE = .RDONLY,
        }, 0) catch {
            posix.close(write_fd);
            return error.IoError;
        };

        // Get file size via fstat
        const stat = posix.fstat(write_fd) catch {
            posix.close(read_fd);
            posix.close(write_fd);
            return error.IoError;
        };
        const size: i64 = @intCast(stat.size);

        const now = @as(i64, @intCast(std.time.timestamp()));

        const self = allocator.create(Extent) catch {
            posix.close(read_fd);
            posix.close(write_fd);
            return error.IoError;
        };
        self.* = Extent{
            .write_fd = write_fd,
            .read_fd = read_fd,
            .file_path = path,
            .extent_id = extent_id,
            .modify_time = std.atomic.Value(i64).init(now),
            .data_size = std.atomic.Value(i64).init(size),
            .has_close = std.atomic.Value(i32).init(0),
            .header = &.{},
            .header_len = 0,
            .header_capacity = 0,
            .dirty = std.atomic.Value(bool).init(false),
            .io_count = std.atomic.Value(u64).init(0),
            .write_mutex = Mutex{},
            .header_lock = RwLock{},
            .allocator = allocator,
        };
        return self;
    }

    /// Write data to the extent using pwrite for atomic positioning.
    pub fn write(self: *Extent, param: *const types.WriteParam) err.Error!u32 {
        if (self.has_close.load(.acquire) == constants.EXTENT_HAS_CLOSE) {
            return error.ExtentDeleted;
        }

        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        const fd = self.write_fd;
        const data = param.data;
        const offset: u64 = @bitCast(param.offset);
        var written: usize = 0;

        while (written < data.len) {
            const result = posix.pwrite(fd, data[written..], offset + written) catch return error.IoError;
            if (result == 0) {
                return error.WriteZero;
            }
            written += result;
        }

        if (param.is_sync) {
            _ = std.posix.system.fdatasync(fd);
        }

        // Update data size — we hold write_mutex so no concurrent writer;
        // use a plain store when the new end is larger.
        const new_end: i64 = param.offset + param.size;
        const current = self.data_size.load(.monotonic);
        if (new_end > current) {
            self.data_size.store(new_end, .release);
        }

        self.dirty.store(true, .release);

        // Coarse-grained modify_time: only update every 256 writes
        const count = self.io_count.fetchAdd(1, .monotonic);
        if (count % 256 == 0) {
            const now = @as(i64, @intCast(std.time.timestamp()));
            self.modify_time.store(now, .monotonic);
        }

        // Update block CRCs — we already hold write_mutex so skip header_lock
        const blocks_written = self.updateBlockCrcsNoLock(param);
        return blocks_written;
    }

    /// Read data from the extent using pread — fully lock-free.
    pub fn read(self: *Extent, offset: u64, buf: []u8) err.Error!usize {
        if (self.has_close.load(.acquire) == constants.EXTENT_HAS_CLOSE) {
            return error.ExtentDeleted;
        }

        return posix.pread(self.read_fd, buf, offset) catch return error.IoError;
    }

    /// Flush any buffered writes to disk using fdatasync.
    pub fn flush(self: *Extent) err.Error!void {
        if (self.dirty.swap(false, .acq_rel)) {
            const ret = std.posix.system.fdatasync(self.write_fd);
            if (ret != 0) {
                self.dirty.store(true, .release);
                return error.IoError;
            }
        }
    }

    /// Close the extent, releasing file handles.
    pub fn close(self: *Extent) err.Error!void {
        self.has_close.store(constants.EXTENT_HAS_CLOSE, .release);
        try self.flush();
    }

    /// Destroy the extent, closing FDs and freeing memory.
    pub fn destroy(self: *Extent) void {
        posix.close(self.write_fd);
        posix.close(self.read_fd);
        if (self.header_capacity > 0) {
            self.allocator.free(self.header[0..self.header_capacity]);
        }
        self.allocator.destroy(self);
    }

    /// Current data size in bytes.
    pub fn dataSize(self: *const Extent) i64 {
        return self.data_size.load(.acquire);
    }

    /// Whether the extent has been closed.
    pub fn isClosed(self: *const Extent) bool {
        return self.has_close.load(.acquire) == constants.EXTENT_HAS_CLOSE;
    }

    /// Last modification time as unix timestamp.
    pub fn modifyTime(self: *const Extent) i64 {
        return self.modify_time.load(.monotonic);
    }

    /// Truncate extent to the given size.
    pub fn truncate(self: *Extent, size: u64) err.Error!void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        posix.ftruncate(self.write_fd, @intCast(size)) catch return error.IoError;
        self.data_size.store(@intCast(size), .release);
        self.dirty.store(true, .release);
    }

    /// Punch a hole in the extent using fallocate (Linux-specific).
    pub fn punchHole(self: *Extent, offset: u64, size: u64) err.Error!void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE = 0x02 | 0x01 = 0x03
        const FALLOC_FL_PUNCH_HOLE: i32 = 0x02;
        const FALLOC_FL_KEEP_SIZE: i32 = 0x01;
        const mode: i32 = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;

        const ret = std.os.linux.fallocate(self.write_fd, mode, @bitCast(offset), @bitCast(size));
        if (ret != 0) {
            return error.IoError;
        }
    }

    /// Raw read file descriptor (for io_uring / sendfile).
    pub fn rawReadFd(self: *const Extent) posix.fd_t {
        return self.read_fd;
    }

    /// Update per-block CRC values in the header (caller must hold write_mutex).
    /// Skips header_lock since write_mutex already serializes all writers.
    fn updateBlockCrcsNoLock(self: *Extent, param: *const types.WriteParam) u32 {
        const block_size = @as(usize, @intCast(constants.BLOCK_SIZE));
        const offset_u64: u64 = @bitCast(param.offset);
        const start_block: usize = @intCast(offset_u64 / constants.BLOCK_SIZE);
        const end_offset: usize = @intCast(offset_u64 + param.data.len);
        const end_block: usize = (end_offset + block_size - 1) / block_size;

        const needed_size = end_block * 4; // 4 bytes per block CRC
        if (self.header_len < needed_size) {
            self.ensureHeaderCapacity(needed_size);
            @memset(self.header[self.header_len..needed_size], 0);
            self.header_len = needed_size;
        }

        // Fast path: if the caller provides a non-zero CRC and the write is
        // block-aligned, store the caller's CRC for each covered block without
        // recomputing.  This matches Go's behavior where PersistenceBlockCrc
        // receives the caller CRC for full-block aligned writes.
        if (param.crc != 0 and (offset_u64 % constants.BLOCK_SIZE == 0)) {
            const crc_bytes = std.mem.toBytes(param.crc);
            var block_idx = start_block;
            while (block_idx < end_block) : (block_idx += 1) {
                const crc_offset = block_idx * 4;
                @memcpy(self.header[crc_offset .. crc_offset + 4], &crc_bytes);
            }
            return @intCast(end_block - start_block);
        }

        // Slow path: compute per-block CRC individually.
        const base_off: usize = @intCast(offset_u64);
        var blocks_written: u32 = 0;
        var block_idx = start_block;
        while (block_idx < end_block) : (block_idx += 1) {
            const data_start = if (block_idx == start_block)
                0
            else
                block_idx * block_size - base_off;
            const data_end = @min(
                (block_idx + 1) * block_size - base_off,
                param.data.len,
            );

            if (data_start < param.data.len) {
                const block_data = param.data[data_start..data_end];
                const crc_val = crc32.hash(block_data);
                const crc_offset = block_idx * 4;
                const crc_bytes = std.mem.toBytes(crc_val);
                @memcpy(self.header[crc_offset .. crc_offset + 4], &crc_bytes);
                blocks_written += 1;
            }
        }

        return blocks_written;
    }

    /// Update per-block CRC values in the header.
    fn updateBlockCrcs(self: *Extent, param: *const types.WriteParam) u32 {
        const block_size = @as(usize, @intCast(constants.BLOCK_SIZE));
        const start_block: usize = @intCast(@divFloor(@as(u64, @bitCast(param.offset)), constants.BLOCK_SIZE));
        const end_offset: usize = @intCast(@as(u64, @bitCast(param.offset)) + param.data.len);
        const end_block: usize = (end_offset + block_size - 1) / block_size;

        self.header_lock.lock();
        defer self.header_lock.unlock();

        const needed_size = end_block * 4; // 4 bytes per block CRC
        if (self.header_len < needed_size) {
            self.ensureHeaderCapacity(needed_size);
            // Zero-fill new bytes
            @memset(self.header[self.header_len..needed_size], 0);
            self.header_len = needed_size;
        }

        var blocks_written: u32 = 0;
        var block_idx = start_block;
        while (block_idx < end_block) : (block_idx += 1) {
            const data_start = if (block_idx == start_block)
                0
            else
                block_idx * block_size - @as(usize, @intCast(@as(u64, @bitCast(param.offset))));
            const data_end = @min(
                (block_idx + 1) * block_size - @as(usize, @intCast(@as(u64, @bitCast(param.offset)))),
                param.data.len,
            );

            if (data_start < param.data.len) {
                const block_data = param.data[data_start..data_end];
                const crc_val = crc32.hash(block_data);
                const crc_offset = block_idx * 4;
                const crc_bytes = std.mem.toBytes(crc_val);
                @memcpy(self.header[crc_offset .. crc_offset + 4], &crc_bytes);
                blocks_written += 1;
            }
        }

        return blocks_written;
    }

    /// Ensure header buffer has at least the given capacity.
    fn ensureHeaderCapacity(self: *Extent, min_cap: usize) void {
        if (self.header_capacity >= min_cap) return;

        var new_cap = if (self.header_capacity == 0) @as(usize, 64) else self.header_capacity;
        while (new_cap < min_cap) {
            new_cap *= 2;
        }

        const new_buf = self.allocator.alloc(u8, new_cap) catch return;
        if (self.header_capacity > 0) {
            @memcpy(new_buf[0..self.header_len], self.header[0..self.header_len]);
            self.allocator.free(self.header[0..self.header_capacity]);
        }
        self.header = new_buf;
        self.header_capacity = new_cap;
    }

    /// Get the CRC for a specific block index.
    pub fn blockCrc(self: *Extent, block_idx: usize) ?u32 {
        self.header_lock.lockShared();
        defer self.header_lock.unlockShared();

        const offset = block_idx * 4;
        if (offset + 4 > self.header_len) {
            return null;
        }
        return std.mem.readInt(u32, self.header[offset..][0..4], .little);
    }

    // ─── O_DIRECT read support ─────────────────────────────────────

    /// Read data using O_DIRECT for bypassing page cache.
    /// Both offset and buf.len must be aligned to PAGE_SIZE (4096).
    /// Returns the number of bytes read.
    pub fn readDirect(self: *Extent, offset: u64, buf: []align(constants.PAGE_SIZE) u8) err.Error!usize {
        if (self.has_close.load(.acquire) == constants.EXTENT_HAS_CLOSE) {
            return error.ExtentDeleted;
        }

        // Validate alignment
        if (offset % constants.PAGE_SIZE != 0) return error.IoError;
        if (buf.len % constants.PAGE_SIZE != 0) return error.IoError;

        // Open a temporary O_DIRECT FD
        const direct_fd = posix.open(self.file_path, .{
            .ACCMODE = .RDONLY,
            .DIRECT = true,
        }, 0) catch return error.IoError;
        defer posix.close(direct_fd);

        return posix.pread(direct_fd, buf, offset) catch return error.IoError;
    }

    /// Read data using O_DIRECT with automatic alignment handling.
    /// Works for any offset/size — internally aligns and copies the relevant portion.
    /// Caller-provided buf does NOT need special alignment.
    pub fn readAligned(self: *Extent, offset: u64, buf: []u8) err.Error!usize {
        if (self.has_close.load(.acquire) == constants.EXTENT_HAS_CLOSE) {
            return error.ExtentDeleted;
        }

        const page_size = constants.PAGE_SIZE;
        const aligned_offset = offset & ~@as(u64, page_size - 1);
        const offset_within_page = offset - aligned_offset;
        const needed = offset_within_page + buf.len;
        const aligned_len = (needed + page_size - 1) & ~@as(usize, page_size - 1);

        // Allocate an aligned buffer
        const aligned_buf = std.heap.page_allocator.alignedAlloc(u8, page_size, aligned_len) catch return error.IoError;
        defer std.heap.page_allocator.free(aligned_buf);

        // Open a temporary O_DIRECT FD
        const direct_fd = posix.open(self.file_path, .{
            .ACCMODE = .RDONLY,
            .DIRECT = true,
        }, 0) catch return error.IoError;
        defer posix.close(direct_fd);

        const bytes_read = posix.pread(direct_fd, aligned_buf, aligned_offset) catch return error.IoError;

        // Copy the requested portion
        if (bytes_read <= offset_within_page) return 0;
        const available = bytes_read - offset_within_page;
        const copy_len = @min(available, buf.len);
        @memcpy(buf[0..copy_len], aligned_buf[offset_within_page..][0..copy_len]);
        return copy_len;
    }

    // ─── Sparse file support ───────────────────────────────────────

    /// A contiguous range of data in a sparse file.
    pub const DataRange = struct {
        offset: u64,
        length: u64,
    };

    /// Find the next data range starting at or after `start_offset`.
    /// Uses SEEK_DATA/SEEK_HOLE to detect sparse regions.
    /// Returns null if no more data exists after start_offset.
    pub fn findNextDataRange(self: *const Extent, start_offset: u64) ?DataRange {
        const fd = self.read_fd;

        // SEEK_DATA = 3, SEEK_HOLE = 4
        const SEEK_DATA: c_int = 3;
        const SEEK_HOLE: c_int = 4;

        // Find start of next data region
        const data_start = std.os.linux.lseek(fd, @bitCast(start_offset), SEEK_DATA);
        if (@as(i64, @bitCast(data_start)) < 0) {
            // ENXIO: no more data after start_offset
            return null;
        }

        // Find end of this data region (start of next hole)
        const hole_start = std.os.linux.lseek(fd, @bitCast(data_start), SEEK_HOLE);
        if (@as(i64, @bitCast(hole_start)) < 0) {
            // No hole found — data extends to EOF
            const size: u64 = @intCast(self.data_size.load(.acquire));
            if (data_start >= size) return null;
            return DataRange{
                .offset = data_start,
                .length = size - data_start,
            };
        }

        if (hole_start <= data_start) return null;

        return DataRange{
            .offset = data_start,
            .length = hole_start - data_start,
        };
    }

    /// Get the actual allocated size on disk (excluding holes).
    /// Uses stat.blocks * 512 which reflects actual disk usage.
    pub fn allocatedSize(self: *const Extent) u64 {
        const stat = posix.fstat(self.read_fd) catch return 0;
        return @intCast(stat.blocks * 512);
    }

    /// Check if this extent is sparse (has holes).
    pub fn isSparse(self: *const Extent) bool {
        const file_size: u64 = @intCast(@max(self.data_size.load(.acquire), 0));
        if (file_size == 0) return false;
        return self.allocatedSize() < file_size;
    }

    /// Enumerate all data ranges in the extent.
    /// Returns a list of DataRange structs. Caller owns the returned slice.
    pub fn getDataRanges(self: *const Extent, allocator: Allocator) ![]DataRange {
        var ranges = std.ArrayList(DataRange).init(allocator);
        errdefer ranges.deinit();

        var offset: u64 = 0;
        while (self.findNextDataRange(offset)) |range| {
            try ranges.append(range);
            offset = range.offset + range.length;
        }

        return ranges.toOwnedSlice();
    }
};
