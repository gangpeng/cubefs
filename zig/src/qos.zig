// qos.zig — Token-bucket rate limiter and concurrency limiter for disk I/O.

const std = @import("std");

/// A simple token-bucket rate limiter.
/// When `rate` is 0, the limiter is unlimited (acquire is a no-op).
pub const RateLimiter = struct {
    rate: u64, // tokens per second; 0 = unlimited
    tokens: f64,
    last_refill: i128, // nanosecond timestamp
    mu: std.Thread.Mutex,

    pub fn init(rate: u64) RateLimiter {
        return .{
            .rate = rate,
            .tokens = @floatFromInt(rate),
            .last_refill = std.time.nanoTimestamp(),
            .mu = .{},
        };
    }

    /// Block until one token is available. No-op when rate == 0.
    pub fn acquire(self: *RateLimiter) void {
        self.acquireN(1);
    }

    /// Block until `n` tokens are available. Used for byte-rate limiting.
    /// No-op when rate == 0.
    pub fn acquireN(self: *RateLimiter, n: u64) void {
        if (self.rate == 0) return;
        if (n == 0) return;

        const needed: f64 = @floatFromInt(n);

        self.mu.lock();
        defer self.mu.unlock();

        self.refill();

        while (self.tokens < needed) {
            self.mu.unlock();
            std.time.sleep(1_000_000); // 1 ms
            self.mu.lock();
            self.refill();
        }
        self.tokens -= needed;
    }

    /// Dynamically update the rate limit.
    pub fn setRate(self: *RateLimiter, new_rate: u64) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.rate = new_rate;
        if (new_rate > 0) {
            const rate_f: f64 = @floatFromInt(new_rate);
            self.tokens = @min(self.tokens, rate_f);
        }
    }

    /// Get the current rate.
    pub fn getRate(self: *RateLimiter) u64 {
        self.mu.lock();
        defer self.mu.unlock();
        return self.rate;
    }

    fn refill(self: *RateLimiter) void {
        const now = std.time.nanoTimestamp();
        const elapsed_ns: f64 = @floatFromInt(now - self.last_refill);
        const rate_f: f64 = @floatFromInt(self.rate);
        const new_tokens = elapsed_ns / 1_000_000_000.0 * rate_f;
        self.tokens = @min(self.tokens + new_tokens, rate_f);
        self.last_refill = now;
    }
};

/// A concurrency limiter that caps the number of in-flight operations.
pub const ConcurrencyLimiter = struct {
    max_concurrent: std.atomic.Value(u32),
    current: std.atomic.Value(u32),

    pub fn init(max: u32) ConcurrencyLimiter {
        return .{
            .max_concurrent = std.atomic.Value(u32).init(max),
            .current = std.atomic.Value(u32).init(0),
        };
    }

    /// Try to acquire a slot. Returns false if at capacity.
    /// When max_concurrent is 0, always returns true (unlimited).
    pub fn acquire(self: *ConcurrencyLimiter) bool {
        const max = self.max_concurrent.load(.acquire);
        if (max == 0) return true; // unlimited

        const cur = self.current.load(.acquire);
        if (cur >= max) return false;

        // CAS to increment
        _ = self.current.fetchAdd(1, .seq_cst);
        return true;
    }

    /// Release a slot.
    pub fn release(self: *ConcurrencyLimiter) void {
        const cur = self.current.load(.acquire);
        if (cur > 0) {
            _ = self.current.fetchSub(1, .seq_cst);
        }
    }

    /// Dynamically update the capacity.
    pub fn setCapacity(self: *ConcurrencyLimiter, cap: u32) void {
        self.max_concurrent.store(cap, .release);
    }

    /// Get the current number of in-flight operations.
    pub fn getCurrent(self: *const ConcurrencyLimiter) u32 {
        return self.current.load(.acquire);
    }
};

/// QoS configuration received from master.
pub const QosConfig = struct {
    // Flow limiters (bytes/sec)
    disk_read_flow: u64 = 0,
    disk_write_flow: u64 = 0,
    disk_async_read_flow: u64 = 0,
    disk_async_write_flow: u64 = 0,

    // IOPS limiters (ops/sec)
    disk_read_iops: u64 = 0,
    disk_write_iops: u64 = 0,
    disk_async_read_iops: u64 = 0,
    disk_async_write_iops: u64 = 0,
    disk_delete_iops: u64 = 0,

    // Concurrency limiters
    disk_read_cc: u32 = 0,
    disk_write_cc: u32 = 0,
    disk_async_read_cc: u32 = 0,
    disk_async_write_cc: u32 = 0,
    disk_delete_cc: u32 = 0,
};

test "rate limiter unlimited" {
    var rl = RateLimiter.init(0);
    // Should return immediately
    rl.acquire();
    rl.acquire();
}

test "rate limiter with rate" {
    var rl = RateLimiter.init(10000); // 10k ops/sec
    rl.acquire();
    rl.acquire();
}

test "rate limiter acquireN" {
    var rl = RateLimiter.init(0); // unlimited
    rl.acquireN(1000);
    rl.acquireN(0);

    var rl2 = RateLimiter.init(100000); // 100k tokens/sec
    rl2.acquireN(10);
    rl2.acquireN(1);
}

test "rate limiter setRate" {
    var rl = RateLimiter.init(1000);
    try std.testing.expectEqual(@as(u64, 1000), rl.getRate());
    rl.setRate(5000);
    try std.testing.expectEqual(@as(u64, 5000), rl.getRate());
    rl.setRate(0);
    try std.testing.expectEqual(@as(u64, 0), rl.getRate());
    rl.acquire(); // should be no-op now
}

test "concurrency limiter unlimited" {
    var cl = ConcurrencyLimiter.init(0); // unlimited
    try std.testing.expect(cl.acquire());
    try std.testing.expect(cl.acquire());
    try std.testing.expect(cl.acquire());
}

test "concurrency limiter with capacity" {
    var cl = ConcurrencyLimiter.init(2);
    try std.testing.expect(cl.acquire());
    try std.testing.expect(cl.acquire());
    try std.testing.expect(!cl.acquire()); // at capacity

    cl.release();
    try std.testing.expect(cl.acquire()); // slot freed
}

test "concurrency limiter setCapacity" {
    var cl = ConcurrencyLimiter.init(1);
    try std.testing.expect(cl.acquire());
    try std.testing.expect(!cl.acquire());

    cl.setCapacity(3);
    try std.testing.expect(cl.acquire()); // now has capacity
}
