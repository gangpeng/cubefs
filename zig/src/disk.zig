// disk.zig — Disk abstraction: space tracking, status, enhanced QoS.
// Mirrors Go's datanode disk management with full QoS (14 limiters).

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const qos = @import("qos.zig");
const RateLimiter = qos.RateLimiter;
const ConcurrencyLimiter = qos.ConcurrencyLimiter;
const QosConfig = qos.QosConfig;

// Linux statvfs binding
const Statvfs = extern struct {
    f_bsize: c_ulong,
    f_frsize: c_ulong,
    f_blocks: c_ulong,
    f_bfree: c_ulong,
    f_bavail: c_ulong,
    f_files: c_ulong,
    f_ffree: c_ulong,
    f_favail: c_ulong,
    f_fsid: c_ulong,
    f_flag: c_ulong,
    f_namemax: c_ulong,
    __f_spare: [6]c_int = .{ 0, 0, 0, 0, 0, 0 },
};
extern "c" fn statvfs(path: [*:0]const u8, buf: *Statvfs) c_int;

pub const DISK_STATUS_GOOD: i32 = 0;
pub const DISK_STATUS_BAD: i32 = -1;

/// Default error threshold before marking disk as bad.
const DEFAULT_MAX_ERR_CNT: u64 = 100;

pub const Disk = struct {
    path: []const u8,
    total: std.atomic.Value(u64),
    used: std.atomic.Value(u64),
    available: std.atomic.Value(u64),
    reserved_space: u64,
    status: std.atomic.Value(i32),
    allocator: Allocator,

    // ── Flow limiters (bytes/sec) ────────────────────────────────
    flow_read: RateLimiter,
    flow_write: RateLimiter,
    flow_async_read: RateLimiter,
    flow_async_write: RateLimiter,

    // ── IOPS limiters (ops/sec) ──────────────────────────────────
    iops_read: RateLimiter,
    iops_write: RateLimiter,
    iops_async_read: RateLimiter,
    iops_async_write: RateLimiter,
    iops_delete: RateLimiter,

    // ── Concurrency limiters ─────────────────────────────────────
    iocc_read: ConcurrencyLimiter,
    iocc_write: ConcurrencyLimiter,
    iocc_async_read: ConcurrencyLimiter,
    iocc_async_write: ConcurrencyLimiter,
    iocc_delete: ConcurrencyLimiter,

    // ── Disk error tracking ──────────────────────────────────────
    read_err_cnt: std.atomic.Value(u64),
    write_err_cnt: std.atomic.Value(u64),
    max_err_cnt: u64,

    // ── QoS enable flag ──────────────────────────────────────────
    qos_enable: std.atomic.Value(bool),

    pub fn init(
        allocator: Allocator,
        path: []const u8,
        reserved_space: u64,
        read_iops: u64,
        write_iops: u64,
    ) !*Disk {
        // Ensure directory exists
        std.fs.cwd().makePath(path) catch {};

        const self = try allocator.create(Disk);
        self.* = .{
            .path = path,
            .total = std.atomic.Value(u64).init(0),
            .used = std.atomic.Value(u64).init(0),
            .available = std.atomic.Value(u64).init(0),
            .reserved_space = reserved_space,
            .status = std.atomic.Value(i32).init(DISK_STATUS_GOOD),
            .allocator = allocator,

            // Flow limiters (initially unlimited)
            .flow_read = RateLimiter.init(0),
            .flow_write = RateLimiter.init(0),
            .flow_async_read = RateLimiter.init(0),
            .flow_async_write = RateLimiter.init(0),

            // IOPS limiters
            .iops_read = RateLimiter.init(read_iops),
            .iops_write = RateLimiter.init(write_iops),
            .iops_async_read = RateLimiter.init(0),
            .iops_async_write = RateLimiter.init(0),
            .iops_delete = RateLimiter.init(0),

            // Concurrency limiters (initially unlimited)
            .iocc_read = ConcurrencyLimiter.init(0),
            .iocc_write = ConcurrencyLimiter.init(0),
            .iocc_async_read = ConcurrencyLimiter.init(0),
            .iocc_async_write = ConcurrencyLimiter.init(0),
            .iocc_delete = ConcurrencyLimiter.init(0),

            // Error tracking
            .read_err_cnt = std.atomic.Value(u64).init(0),
            .write_err_cnt = std.atomic.Value(u64).init(0),
            .max_err_cnt = DEFAULT_MAX_ERR_CNT,

            // QoS disabled by default
            .qos_enable = std.atomic.Value(bool).init(false),
        };

        try self.updateSpace();
        return self;
    }

    pub fn deinit(self: *Disk) void {
        self.allocator.destroy(self);
    }

    /// Update space statistics from the filesystem (statvfs).
    pub fn updateSpace(self: *Disk) !void {
        const stat = try statFs(self.path);
        self.total.store(stat.total, .release);
        self.available.store(stat.available, .release);
        if (stat.total >= stat.available) {
            self.used.store(stat.total - stat.available, .release);
        }
    }

    pub fn getTotal(self: *const Disk) u64 {
        return self.total.load(.acquire);
    }

    pub fn getUsed(self: *const Disk) u64 {
        return self.used.load(.acquire);
    }

    pub fn getAvailable(self: *const Disk) u64 {
        return self.available.load(.acquire);
    }

    /// Returns true if the disk has at least `required` free bytes beyond reserved space.
    pub fn hasSpace(self: *const Disk, required: u64) bool {
        const avail = self.available.load(.acquire);
        if (avail <= self.reserved_space) return false;
        return (avail - self.reserved_space) >= required;
    }

    pub fn isGood(self: *const Disk) bool {
        return self.status.load(.acquire) == DISK_STATUS_GOOD;
    }

    pub fn setStatus(self: *Disk, status: i32) void {
        self.status.store(status, .release);
    }

    // ── Simple acquire methods (backward compat) ─────────────────

    pub fn acquireRead(self: *Disk) void {
        self.iops_read.acquire();
    }

    pub fn acquireWrite(self: *Disk) void {
        self.iops_write.acquire();
    }

    pub fn acquireDelete(self: *Disk) void {
        self.iops_delete.acquire();
    }

    // ── Full QoS acquire/release ─────────────────────────────────

    /// Acquire all read QoS limiters (flow + IOPS + concurrency).
    pub fn acquireReadQos(self: *Disk, size: u64) void {
        if (!self.qos_enable.load(.acquire)) {
            self.iops_read.acquire();
            return;
        }
        self.flow_read.acquireN(size);
        self.iops_read.acquire();
        _ = self.iocc_read.acquire();
    }

    /// Release read concurrency slot.
    pub fn releaseReadQos(self: *Disk) void {
        if (self.qos_enable.load(.acquire)) {
            self.iocc_read.release();
        }
    }

    /// Acquire all write QoS limiters (flow + IOPS + concurrency).
    pub fn acquireWriteQos(self: *Disk, size: u64) void {
        if (!self.qos_enable.load(.acquire)) {
            self.iops_write.acquire();
            return;
        }
        self.flow_write.acquireN(size);
        self.iops_write.acquire();
        _ = self.iocc_write.acquire();
    }

    /// Release write concurrency slot.
    pub fn releaseWriteQos(self: *Disk) void {
        if (self.qos_enable.load(.acquire)) {
            self.iocc_write.release();
        }
    }

    /// Acquire all delete QoS limiters.
    pub fn acquireDeleteQos(self: *Disk) void {
        self.iops_delete.acquire();
        if (self.qos_enable.load(.acquire)) {
            _ = self.iocc_delete.acquire();
        }
    }

    /// Release delete concurrency slot.
    pub fn releaseDeleteQos(self: *Disk) void {
        if (self.qos_enable.load(.acquire)) {
            self.iocc_delete.release();
        }
    }

    /// Update all QoS limits from a config received from master.
    pub fn updateQosLimits(self: *Disk, config: QosConfig) void {
        // Flow limiters
        self.flow_read.setRate(config.disk_read_flow);
        self.flow_write.setRate(config.disk_write_flow);
        self.flow_async_read.setRate(config.disk_async_read_flow);
        self.flow_async_write.setRate(config.disk_async_write_flow);

        // IOPS limiters
        self.iops_read.setRate(config.disk_read_iops);
        self.iops_write.setRate(config.disk_write_iops);
        self.iops_async_read.setRate(config.disk_async_read_iops);
        self.iops_async_write.setRate(config.disk_async_write_iops);
        self.iops_delete.setRate(config.disk_delete_iops);

        // Concurrency limiters
        self.iocc_read.setCapacity(config.disk_read_cc);
        self.iocc_write.setCapacity(config.disk_write_cc);
        self.iocc_async_read.setCapacity(config.disk_async_read_cc);
        self.iocc_async_write.setCapacity(config.disk_async_write_cc);
        self.iocc_delete.setCapacity(config.disk_delete_cc);

        self.qos_enable.store(true, .release);
    }

    /// Track a disk error and check if we've exceeded the threshold.
    pub fn checkDiskError(self: *Disk, is_read: bool) void {
        if (is_read) {
            const cnt = self.read_err_cnt.fetchAdd(1, .seq_cst) + 1;
            if (cnt >= self.max_err_cnt) {
                self.setStatus(DISK_STATUS_BAD);
            }
        } else {
            const cnt = self.write_err_cnt.fetchAdd(1, .seq_cst) + 1;
            if (cnt >= self.max_err_cnt) {
                self.setStatus(DISK_STATUS_BAD);
            }
        }
    }

    /// Get the total read error count.
    pub fn getReadErrCnt(self: *const Disk) u64 {
        return self.read_err_cnt.load(.acquire);
    }

    /// Get the total write error count.
    pub fn getWriteErrCnt(self: *const Disk) u64 {
        return self.write_err_cnt.load(.acquire);
    }

    /// Get filesystem stats for a path using statvfs.
    fn statFs(path: []const u8) !struct { total: u64, available: u64 } {
        // Use a stack buffer for null-terminated path
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return error.NameTooLong;
        @memcpy(path_buf[0..path.len], path);
        path_buf[path.len] = 0;
        const path_z: [*:0]const u8 = @ptrCast(&path_buf);

        var stat: Statvfs = undefined;
        const rc = statvfs(path_z, &stat);
        if (rc != 0) return error.AccessDenied;

        const block_size: u64 = stat.f_frsize;
        const total = block_size * stat.f_blocks;
        const available = block_size * stat.f_bavail;
        return .{ .total = total, .available = available };
    }
};
