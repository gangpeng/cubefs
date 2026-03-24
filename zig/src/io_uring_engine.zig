// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! io_uring batched I/O engine for high-IOPS extent operations.
//!
//! Provides a submission-queue based I/O engine that batches multiple
//! pread/pwrite calls into a single io_uring submission.

const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;
const err = @import("error.zig");

/// Default submission queue depth.
const DEFAULT_SQ_DEPTH: u16 = 256;

/// A completed I/O operation result.
pub const IoCompletion = struct {
    /// User-provided ID to correlate completions with submissions.
    user_data: u64,
    /// Number of bytes transferred (positive) or negated errno (negative).
    result: i32,
};

/// io_uring-based batched I/O engine.
pub const UringEngine = struct {
    ring: linux.IoUring,
    mutex: std.Thread.Mutex,

    /// Create a new io_uring engine with the default queue depth (256).
    pub fn init() !UringEngine {
        return initWithDepth(DEFAULT_SQ_DEPTH);
    }

    /// Create a new io_uring engine with a custom queue depth.
    pub fn initWithDepth(depth: u16) !UringEngine {
        const ring = try linux.IoUring.init(depth, 0);
        return UringEngine{
            .ring = ring,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *UringEngine) void {
        self.ring.deinit();
    }

    /// Submit a single read operation and wait for completion.
    pub fn read(self: *UringEngine, fd: posix.fd_t, offset: u64, buf: []u8) err.Error!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var sqe = self.ring.get_sqe() catch return error.SubmissionQueueFull;
        sqe.prep_read(fd, buf, offset);
        sqe.user_data = 0;

        _ = self.ring.submit_and_wait(1) catch return error.IoError;

        var cqes: [1]linux.io_uring_cqe = undefined;
        const count = self.ring.copy_cqes(&cqes, 1) catch return error.NoCompletion;
        if (count == 0) return error.NoCompletion;

        const result = cqes[0].res;
        if (result < 0) return error.IoError;

        return @intCast(result);
    }

    /// Submit a single write operation and wait for completion.
    pub fn write(self: *UringEngine, fd: posix.fd_t, offset: u64, data: []const u8) err.Error!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var sqe = self.ring.get_sqe() catch return error.SubmissionQueueFull;
        sqe.prep_write(fd, data, offset);
        sqe.user_data = 0;

        _ = self.ring.submit_and_wait(1) catch return error.IoError;

        var cqes: [1]linux.io_uring_cqe = undefined;
        const count = self.ring.copy_cqes(&cqes, 1) catch return error.NoCompletion;
        if (count == 0) return error.NoCompletion;

        const result = cqes[0].res;
        if (result < 0) return error.IoError;

        return @intCast(result);
    }

    /// Submit a single fsync operation for an FD.
    pub fn fsync(self: *UringEngine, fd: posix.fd_t) err.Error!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var sqe = self.ring.get_sqe() catch return error.SubmissionQueueFull;
        sqe.prep_fsync(fd, 0);
        sqe.user_data = 0;

        _ = self.ring.submit_and_wait(1) catch return error.IoError;

        var cqes: [1]linux.io_uring_cqe = undefined;
        const count = self.ring.copy_cqes(&cqes, 1) catch return error.NoCompletion;
        if (count == 0) return error.NoCompletion;

        if (cqes[0].res < 0) return error.IoError;
    }

    /// Submit a batch of read operations and wait for all completions.
    pub fn submitReads(
        self: *UringEngine,
        ops: []const struct { fd: posix.fd_t, offset: u64, buf: []u8, user_data: u64 },
    ) err.Error![]IoCompletion {
        if (ops.len == 0) return &.{};

        self.mutex.lock();
        defer self.mutex.unlock();

        for (ops) |op| {
            var sqe = self.ring.get_sqe() catch return error.SubmissionQueueFull;
            sqe.prep_read(op.fd, op.buf, op.offset);
            sqe.user_data = op.user_data;
        }

        _ = self.ring.submit_and_wait(@intCast(ops.len)) catch return error.IoError;

        // Collect completions
        var completions: [256]IoCompletion = undefined;
        var total: usize = 0;
        while (total < ops.len) {
            var cqes: [256]linux.io_uring_cqe = undefined;
            const count = self.ring.copy_cqes(&cqes, 1) catch break;
            for (cqes[0..count]) |cqe| {
                if (total < completions.len) {
                    completions[total] = IoCompletion{
                        .user_data = cqe.user_data,
                        .result = cqe.res,
                    };
                    total += 1;
                }
            }
        }

        return completions[0..total];
    }

    /// Submit a batch of write operations and wait for all completions.
    pub fn submitWrites(
        self: *UringEngine,
        ops: []const struct { fd: posix.fd_t, offset: u64, data: []const u8, user_data: u64 },
    ) err.Error![]IoCompletion {
        if (ops.len == 0) return &.{};

        self.mutex.lock();
        defer self.mutex.unlock();

        for (ops) |op| {
            var sqe = self.ring.get_sqe() catch return error.SubmissionQueueFull;
            sqe.prep_write(op.fd, op.data, op.offset);
            sqe.user_data = op.user_data;
        }

        _ = self.ring.submit_and_wait(@intCast(ops.len)) catch return error.IoError;

        var completions: [256]IoCompletion = undefined;
        var total: usize = 0;
        while (total < ops.len) {
            var cqes: [256]linux.io_uring_cqe = undefined;
            const count = self.ring.copy_cqes(&cqes, 1) catch break;
            for (cqes[0..count]) |cqe| {
                if (total < completions.len) {
                    completions[total] = IoCompletion{
                        .user_data = cqe.user_data,
                        .result = cqe.res,
                    };
                    total += 1;
                }
            }
        }

        return completions[0..total];
    }
};
