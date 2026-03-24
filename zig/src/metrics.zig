// metrics.zig — I/O metrics collection for the datanode.
// Tracks bytes, ops, errors, latency, and active connections.

const std = @import("std");

/// Atomic I/O metrics for the datanode.
pub const Metrics = struct {
    bytes_read: std.atomic.Value(u64),
    bytes_written: std.atomic.Value(u64),
    read_ops: std.atomic.Value(u64),
    write_ops: std.atomic.Value(u64),
    delete_ops: std.atomic.Value(u64),
    read_errors: std.atomic.Value(u64),
    write_errors: std.atomic.Value(u64),
    active_connections: std.atomic.Value(u32),

    pub fn init() Metrics {
        return .{
            .bytes_read = std.atomic.Value(u64).init(0),
            .bytes_written = std.atomic.Value(u64).init(0),
            .read_ops = std.atomic.Value(u64).init(0),
            .write_ops = std.atomic.Value(u64).init(0),
            .delete_ops = std.atomic.Value(u64).init(0),
            .read_errors = std.atomic.Value(u64).init(0),
            .write_errors = std.atomic.Value(u64).init(0),
            .active_connections = std.atomic.Value(u32).init(0),
        };
    }

    /// Record a successful read operation.
    pub fn recordRead(self: *Metrics, bytes: u64) void {
        _ = self.read_ops.fetchAdd(1, .monotonic);
        _ = self.bytes_read.fetchAdd(bytes, .monotonic);
    }

    /// Record a successful write operation.
    pub fn recordWrite(self: *Metrics, bytes: u64) void {
        _ = self.write_ops.fetchAdd(1, .monotonic);
        _ = self.bytes_written.fetchAdd(bytes, .monotonic);
    }

    /// Record a delete operation.
    pub fn recordDelete(self: *Metrics) void {
        _ = self.delete_ops.fetchAdd(1, .monotonic);
    }

    /// Record an error.
    pub fn recordError(self: *Metrics, is_read: bool) void {
        if (is_read) {
            _ = self.read_errors.fetchAdd(1, .monotonic);
        } else {
            _ = self.write_errors.fetchAdd(1, .monotonic);
        }
    }

    /// Increment active connection count.
    pub fn connectionOpened(self: *Metrics) void {
        _ = self.active_connections.fetchAdd(1, .monotonic);
    }

    /// Decrement active connection count.
    pub fn connectionClosed(self: *Metrics) void {
        _ = self.active_connections.fetchSub(1, .monotonic);
    }

    /// Take a snapshot of all metrics.
    pub fn snapshot(self: *const Metrics) MetricsSnapshot {
        return .{
            .bytes_read = self.bytes_read.load(.monotonic),
            .bytes_written = self.bytes_written.load(.monotonic),
            .read_ops = self.read_ops.load(.monotonic),
            .write_ops = self.write_ops.load(.monotonic),
            .delete_ops = self.delete_ops.load(.monotonic),
            .read_errors = self.read_errors.load(.monotonic),
            .write_errors = self.write_errors.load(.monotonic),
            .active_connections = self.active_connections.load(.monotonic),
        };
    }

    /// Reset all counters to zero.
    pub fn reset(self: *Metrics) void {
        self.bytes_read.store(0, .monotonic);
        self.bytes_written.store(0, .monotonic);
        self.read_ops.store(0, .monotonic);
        self.write_ops.store(0, .monotonic);
        self.delete_ops.store(0, .monotonic);
        self.read_errors.store(0, .monotonic);
        self.write_errors.store(0, .monotonic);
    }
};

/// Immutable snapshot of metrics at a point in time.
pub const MetricsSnapshot = struct {
    bytes_read: u64,
    bytes_written: u64,
    read_ops: u64,
    write_ops: u64,
    delete_ops: u64,
    read_errors: u64,
    write_errors: u64,
    active_connections: u32,
};

/// Global metrics instance.
pub var global: Metrics = Metrics.init();

// ─── Latency Histogram ───────────────────────────────────────────────

/// Number of histogram buckets.
const BUCKET_COUNT: usize = 9;

/// Bucket boundaries in microseconds.
/// [50us, 100us, 250us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms]
const BUCKET_BOUNDS: [BUCKET_COUNT]u64 = .{
    50, 100, 250, 500, 1_000, 5_000, 10_000, 50_000, 100_000,
};

/// Lock-free latency histogram with fixed bucket boundaries.
/// Each bucket counts observations <= its boundary. Thread-safe via atomics.
pub const LatencyHistogram = struct {
    buckets: [BUCKET_COUNT]std.atomic.Value(u64),
    inf_bucket: std.atomic.Value(u64),
    sum_us: std.atomic.Value(u64),
    count: std.atomic.Value(u64),

    pub fn init() LatencyHistogram {
        var h: LatencyHistogram = undefined;
        for (&h.buckets) |*b| {
            b.* = std.atomic.Value(u64).init(0);
        }
        h.inf_bucket = std.atomic.Value(u64).init(0);
        h.sum_us = std.atomic.Value(u64).init(0);
        h.count = std.atomic.Value(u64).init(0);
        return h;
    }

    /// Record a latency observation in microseconds.
    pub fn observe(self: *LatencyHistogram, latency_us: u64) void {
        _ = self.count.fetchAdd(1, .monotonic);
        _ = self.sum_us.fetchAdd(latency_us, .monotonic);

        var found = false;
        for (&self.buckets, 0..) |*b, i| {
            if (latency_us <= BUCKET_BOUNDS[i]) {
                _ = b.fetchAdd(1, .monotonic);
                found = true;
                break;
            }
        }
        if (!found) {
            _ = self.inf_bucket.fetchAdd(1, .monotonic);
        }
    }

    /// Record latency from nanosecond timer start/end.
    pub fn observeNs(self: *LatencyHistogram, start_ns: i128, end_ns: i128) void {
        if (end_ns <= start_ns) {
            self.observe(0);
            return;
        }
        const diff_ns: u64 = @intCast(end_ns - start_ns);
        const diff_us = diff_ns / 1_000;
        self.observe(diff_us);
    }

    /// Take a snapshot of the histogram.
    pub fn snapshot(self: *const LatencyHistogram) HistogramSnapshot {
        var snap: HistogramSnapshot = undefined;
        for (&self.buckets, 0..) |*b, i| {
            snap.buckets[i] = b.load(.monotonic);
        }
        snap.inf_bucket = self.inf_bucket.load(.monotonic);
        snap.sum_us = self.sum_us.load(.monotonic);
        snap.count = self.count.load(.monotonic);
        return snap;
    }
};

/// Immutable snapshot of a LatencyHistogram.
pub const HistogramSnapshot = struct {
    buckets: [BUCKET_COUNT]u64,
    inf_bucket: u64,
    sum_us: u64,
    count: u64,

    /// Format this histogram as Prometheus text format lines.
    /// `name` is the metric name (e.g. "cubefs_dn_read_latency").
    pub fn writePrometheus(self: *const HistogramSnapshot, writer: anytype, name: []const u8) !void {
        var cumulative: u64 = 0;
        for (0..BUCKET_COUNT) |i| {
            cumulative += self.buckets[i];
            try writer.print("{s}_bucket{{le=\"{d}\"}} {d}\n", .{ name, BUCKET_BOUNDS[i], cumulative });
        }
        cumulative += self.inf_bucket;
        try writer.print("{s}_bucket{{le=\"+Inf\"}} {d}\n", .{ name, cumulative });
        // Convert sum from microseconds to seconds for Prometheus
        const sum_sec_whole = self.sum_us / 1_000_000;
        const sum_sec_frac = (self.sum_us % 1_000_000) / 1_000;
        try writer.print("{s}_sum {d}.{d:0>3}\n", .{ name, sum_sec_whole, sum_sec_frac });
        try writer.print("{s}_count {d}\n", .{ name, self.count });
    }
};

/// Global latency histograms.
pub var read_latency: LatencyHistogram = LatencyHistogram.init();
pub var write_latency: LatencyHistogram = LatencyHistogram.init();

/// Format all metrics in Prometheus exposition format.
pub fn formatPrometheus(allocator: std.mem.Allocator) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    const writer = buf.writer();

    const snap = global.snapshot();

    // Gauge / counter metrics
    try writer.print("# HELP cubefs_dn_bytes_read Total bytes read.\n", .{});
    try writer.print("# TYPE cubefs_dn_bytes_read counter\n", .{});
    try writer.print("cubefs_dn_bytes_read {d}\n", .{snap.bytes_read});

    try writer.print("# HELP cubefs_dn_bytes_written Total bytes written.\n", .{});
    try writer.print("# TYPE cubefs_dn_bytes_written counter\n", .{});
    try writer.print("cubefs_dn_bytes_written {d}\n", .{snap.bytes_written});

    try writer.print("# HELP cubefs_dn_read_ops Total read operations.\n", .{});
    try writer.print("# TYPE cubefs_dn_read_ops counter\n", .{});
    try writer.print("cubefs_dn_read_ops {d}\n", .{snap.read_ops});

    try writer.print("# HELP cubefs_dn_write_ops Total write operations.\n", .{});
    try writer.print("# TYPE cubefs_dn_write_ops counter\n", .{});
    try writer.print("cubefs_dn_write_ops {d}\n", .{snap.write_ops});

    try writer.print("# HELP cubefs_dn_delete_ops Total delete operations.\n", .{});
    try writer.print("# TYPE cubefs_dn_delete_ops counter\n", .{});
    try writer.print("cubefs_dn_delete_ops {d}\n", .{snap.delete_ops});

    try writer.print("# HELP cubefs_dn_read_errors Total read errors.\n", .{});
    try writer.print("# TYPE cubefs_dn_read_errors counter\n", .{});
    try writer.print("cubefs_dn_read_errors {d}\n", .{snap.read_errors});

    try writer.print("# HELP cubefs_dn_write_errors Total write errors.\n", .{});
    try writer.print("# TYPE cubefs_dn_write_errors counter\n", .{});
    try writer.print("cubefs_dn_write_errors {d}\n", .{snap.write_errors});

    try writer.print("# HELP cubefs_dn_active_connections Current active connections.\n", .{});
    try writer.print("# TYPE cubefs_dn_active_connections gauge\n", .{});
    try writer.print("cubefs_dn_active_connections {d}\n", .{snap.active_connections});

    // Latency histograms
    try writer.print("# HELP cubefs_dn_read_latency_us Read latency in microseconds.\n", .{});
    try writer.print("# TYPE cubefs_dn_read_latency_us histogram\n", .{});
    const read_snap = read_latency.snapshot();
    try read_snap.writePrometheus(writer, "cubefs_dn_read_latency_us");

    try writer.print("# HELP cubefs_dn_write_latency_us Write latency in microseconds.\n", .{});
    try writer.print("# TYPE cubefs_dn_write_latency_us histogram\n", .{});
    const write_snap = write_latency.snapshot();
    try write_snap.writePrometheus(writer, "cubefs_dn_write_latency_us");

    return buf.toOwnedSlice();
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "Metrics init has zero values" {
    const m = Metrics.init();
    const snap = m.snapshot();
    try testing.expectEqual(@as(u64, 0), snap.bytes_read);
    try testing.expectEqual(@as(u64, 0), snap.bytes_written);
    try testing.expectEqual(@as(u64, 0), snap.read_ops);
    try testing.expectEqual(@as(u64, 0), snap.write_ops);
    try testing.expectEqual(@as(u64, 0), snap.delete_ops);
    try testing.expectEqual(@as(u64, 0), snap.read_errors);
    try testing.expectEqual(@as(u64, 0), snap.write_errors);
    try testing.expectEqual(@as(u32, 0), snap.active_connections);
}

test "Metrics recordRead increments counters" {
    var m = Metrics.init();
    m.recordRead(1024);
    m.recordRead(2048);

    const snap = m.snapshot();
    try testing.expectEqual(@as(u64, 2), snap.read_ops);
    try testing.expectEqual(@as(u64, 3072), snap.bytes_read);
}

test "Metrics recordWrite increments counters" {
    var m = Metrics.init();
    m.recordWrite(4096);

    const snap = m.snapshot();
    try testing.expectEqual(@as(u64, 1), snap.write_ops);
    try testing.expectEqual(@as(u64, 4096), snap.bytes_written);
}

test "Metrics recordDelete increments counter" {
    var m = Metrics.init();
    m.recordDelete();
    m.recordDelete();
    m.recordDelete();

    const snap = m.snapshot();
    try testing.expectEqual(@as(u64, 3), snap.delete_ops);
}

test "Metrics recordError tracks read and write errors" {
    var m = Metrics.init();
    m.recordError(true);
    m.recordError(true);
    m.recordError(false);

    const snap = m.snapshot();
    try testing.expectEqual(@as(u64, 2), snap.read_errors);
    try testing.expectEqual(@as(u64, 1), snap.write_errors);
}

test "Metrics connection tracking" {
    var m = Metrics.init();
    m.connectionOpened();
    m.connectionOpened();
    m.connectionOpened();
    try testing.expectEqual(@as(u32, 3), m.snapshot().active_connections);

    m.connectionClosed();
    try testing.expectEqual(@as(u32, 2), m.snapshot().active_connections);
}

test "Metrics reset zeroes all counters" {
    var m = Metrics.init();
    m.recordRead(100);
    m.recordWrite(200);
    m.recordDelete();
    m.recordError(true);

    m.reset();

    const snap = m.snapshot();
    try testing.expectEqual(@as(u64, 0), snap.bytes_read);
    try testing.expectEqual(@as(u64, 0), snap.bytes_written);
    try testing.expectEqual(@as(u64, 0), snap.read_ops);
    try testing.expectEqual(@as(u64, 0), snap.write_ops);
    try testing.expectEqual(@as(u64, 0), snap.delete_ops);
}

test "LatencyHistogram observe places into correct bucket" {
    var h = LatencyHistogram.init();

    // 30us → bucket 0 (le=50)
    h.observe(30);
    // 80us → bucket 1 (le=100)
    h.observe(80);
    // 600us → bucket 3 (le=500 is too small, le=1000 fits)
    h.observe(600);
    // 200000us → +Inf bucket
    h.observe(200_000);

    const snap = h.snapshot();
    try testing.expectEqual(@as(u64, 4), snap.count);
    try testing.expectEqual(@as(u64, 30 + 80 + 600 + 200_000), snap.sum_us);
    try testing.expectEqual(@as(u64, 1), snap.buckets[0]); // le=50
    try testing.expectEqual(@as(u64, 1), snap.buckets[1]); // le=100
    try testing.expectEqual(@as(u64, 0), snap.buckets[2]); // le=250
    try testing.expectEqual(@as(u64, 0), snap.buckets[3]); // le=500
    try testing.expectEqual(@as(u64, 1), snap.buckets[4]); // le=1000
    try testing.expectEqual(@as(u64, 1), snap.inf_bucket); // +Inf
}

test "LatencyHistogram observeNs converts correctly" {
    var h = LatencyHistogram.init();

    // 500ns = 0us
    h.observeNs(1000, 1500);
    // 5000ns = 5us → bucket 0 (le=50)
    h.observeNs(0, 5_000);

    const snap = h.snapshot();
    try testing.expectEqual(@as(u64, 2), snap.count);
    try testing.expectEqual(@as(u64, 5), snap.sum_us); // 0 + 5
}

test "formatPrometheus produces valid output" {
    const allocator = testing.allocator;

    // Reset global state for test
    global.reset();

    global.recordRead(1024);
    global.recordWrite(2048);
    global.recordDelete();
    global.recordError(true);

    const output = try formatPrometheus(allocator);
    defer allocator.free(output);

    // Verify the output contains expected metric names
    try testing.expect(std.mem.indexOf(u8, output, "cubefs_dn_bytes_read 1024") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cubefs_dn_bytes_written 2048") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cubefs_dn_read_ops 1") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cubefs_dn_write_ops 1") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cubefs_dn_delete_ops 1") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cubefs_dn_read_errors 1") != null);
    try testing.expect(std.mem.indexOf(u8, output, "# TYPE cubefs_dn_read_latency_us histogram") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cubefs_dn_read_latency_us_bucket{le=\"+Inf\"}") != null);

    // Reset for other tests
    global.reset();
}
