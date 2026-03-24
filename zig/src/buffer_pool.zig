// buffer_pool.zig — Size-bucketed buffer recycling pool.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Size buckets for pooled buffers.
const BUCKET_SIZES = [_]usize{
    4 * 1024, // 4 KB
    64 * 1024, // 64 KB
    128 * 1024, // 128 KB
    256 * 1024, // 256 KB
    1024 * 1024, // 1 MB
    2 * 1024 * 1024, // 2 MB
};

const MAX_PER_BUCKET = 32;

const Bucket = struct {
    mu: std.Thread.Mutex = .{},
    buffers: std.ArrayList([]u8),
    size: usize,

    fn init(allocator: Allocator, size: usize) Bucket {
        return .{
            .buffers = std.ArrayList([]u8).init(allocator),
            .size = size,
        };
    }

    fn deinit(self: *Bucket, allocator: Allocator) void {
        for (self.buffers.items) |buf| {
            allocator.free(buf);
        }
        self.buffers.deinit();
    }
};

pub const BufferPool = struct {
    buckets: [BUCKET_SIZES.len]Bucket,
    allocator: Allocator,

    pub fn init(allocator: Allocator) BufferPool {
        var buckets: [BUCKET_SIZES.len]Bucket = undefined;
        for (&buckets, BUCKET_SIZES) |*b, size| {
            b.* = Bucket.init(allocator, size);
        }
        return .{
            .buckets = buckets,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BufferPool) void {
        for (&self.buckets) |*b| {
            b.deinit(self.allocator);
        }
    }

    /// Get a buffer of at least `size` bytes. Returns from pool or allocates new.
    pub fn getBuffer(self: *BufferPool, size: usize) ![]u8 {
        const bucket_idx = findBucket(size);
        if (bucket_idx) |idx| {
            var bucket = &self.buckets[idx];
            bucket.mu.lock();
            defer bucket.mu.unlock();
            if (bucket.buffers.items.len > 0) {
                return bucket.buffers.pop().?;
            }
            // Allocate new buffer of bucket size
            return try self.allocator.alloc(u8, bucket.size);
        }
        // Size exceeds all buckets; allocate directly
        return try self.allocator.alloc(u8, size);
    }

    /// Return a buffer to the pool for reuse.
    pub fn putBuffer(self: *BufferPool, buf: []u8) void {
        const bucket_idx = findBucket(buf.len);
        if (bucket_idx) |idx| {
            if (buf.len == self.buckets[idx].size) {
                var bucket = &self.buckets[idx];
                bucket.mu.lock();
                defer bucket.mu.unlock();
                if (bucket.buffers.items.len < MAX_PER_BUCKET) {
                    bucket.buffers.append(buf) catch {
                        self.allocator.free(buf);
                    };
                    return;
                }
            }
        }
        // Can't pool it; free directly
        self.allocator.free(buf);
    }

    fn findBucket(size: usize) ?usize {
        for (BUCKET_SIZES, 0..) |bs, i| {
            if (size <= bs) return i;
        }
        return null;
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "BufferPool getBuffer returns correct size for small request" {
    var pool = BufferPool.init(testing.allocator);
    defer pool.deinit();

    // Request 100 bytes → gets 4 KB bucket buffer
    const buf = try pool.getBuffer(100);
    defer pool.putBuffer(buf);

    try testing.expectEqual(@as(usize, 4 * 1024), buf.len);
}

test "BufferPool getBuffer returns exact bucket size" {
    var pool = BufferPool.init(testing.allocator);
    defer pool.deinit();

    // Request exactly 64 KB → gets 64 KB bucket buffer
    const buf = try pool.getBuffer(64 * 1024);
    defer pool.putBuffer(buf);

    try testing.expectEqual(@as(usize, 64 * 1024), buf.len);
}

test "BufferPool getBuffer returns direct alloc for oversized request" {
    var pool = BufferPool.init(testing.allocator);
    defer pool.deinit();

    // Request larger than any bucket → allocated directly
    const size = 4 * 1024 * 1024; // 4 MB
    const buf = try pool.getBuffer(size);
    // Can't putBuffer since it won't match any bucket; free directly
    testing.allocator.free(buf);

    try testing.expectEqual(@as(usize, size), buf.len);
}

test "BufferPool putBuffer recycles and getBuffer reuses" {
    var pool = BufferPool.init(testing.allocator);
    defer pool.deinit();

    // Get a buffer and put it back
    const buf1 = try pool.getBuffer(4 * 1024);
    const buf1_ptr = buf1.ptr;
    pool.putBuffer(buf1);

    // Get another buffer of same size — should reuse the pooled one
    const buf2 = try pool.getBuffer(4 * 1024);
    defer pool.putBuffer(buf2);

    try testing.expectEqual(buf1_ptr, buf2.ptr);
}

test "BufferPool putBuffer frees non-matching sizes" {
    var pool = BufferPool.init(testing.allocator);
    defer pool.deinit();

    // Allocate a buffer that doesn't match any bucket exactly
    const buf = try testing.allocator.alloc(u8, 5000);
    // putBuffer should free it since 5000 != 4096 (bucket size)
    pool.putBuffer(buf);
}

test "BufferPool findBucket returns correct indices" {
    // 1 byte → bucket 0 (4 KB)
    try testing.expectEqual(@as(?usize, 0), BufferPool.findBucket(1));
    // 4 KB → bucket 0
    try testing.expectEqual(@as(?usize, 0), BufferPool.findBucket(4 * 1024));
    // 4 KB + 1 → bucket 1 (64 KB)
    try testing.expectEqual(@as(?usize, 1), BufferPool.findBucket(4 * 1024 + 1));
    // 2 MB → bucket 5
    try testing.expectEqual(@as(?usize, 5), BufferPool.findBucket(2 * 1024 * 1024));
    // > 2 MB → null
    try testing.expectEqual(@as(?usize, null), BufferPool.findBucket(2 * 1024 * 1024 + 1));
}

test "BufferPool init and deinit with empty pool" {
    var pool = BufferPool.init(testing.allocator);
    pool.deinit();
}

test "BufferPool multiple get and put cycles" {
    var pool = BufferPool.init(testing.allocator);
    defer pool.deinit();

    // Get and put multiple buffers of different sizes
    const buf_4k = try pool.getBuffer(1024);
    const buf_64k = try pool.getBuffer(32 * 1024);
    const buf_128k = try pool.getBuffer(65 * 1024);

    pool.putBuffer(buf_4k);
    pool.putBuffer(buf_64k);
    pool.putBuffer(buf_128k);

    // Get them back — should reuse pooled buffers
    const reuse_4k = try pool.getBuffer(100);
    const reuse_64k = try pool.getBuffer(5000);
    const reuse_128k = try pool.getBuffer(100 * 1024);

    pool.putBuffer(reuse_4k);
    pool.putBuffer(reuse_64k);
    pool.putBuffer(reuse_128k);
}
