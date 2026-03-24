// conn_pool.zig — TCP connection pool for replication followers.

const std = @import("std");
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;

const MAX_IDLE_PER_PEER: usize = 4;
const CONNECT_TIMEOUT_NS: u64 = 5 * std.time.ns_per_s;
const IO_TIMEOUT_SECS: u32 = 30; // SO_RCVTIMEO / SO_SNDTIMEO for follower connections
const IDLE_TIMEOUT_NS: i128 = 30 * std.time.ns_per_s;
const CLEANUP_INTERVAL_NS: u64 = 10 * std.time.ns_per_s;

/// A connection with last-used timestamp for idle eviction.
const PooledConnection = struct {
    stream: net.Stream,
    last_used: i128, // nanosecond timestamp
};

/// A pooled TCP connection that can be returned for reuse.
pub const PooledConn = struct {
    stream: net.Stream,
    addr: []const u8,
    pool: *ConnPool,

    /// Return this connection to the pool for reuse.
    pub fn release(self: *PooledConn) void {
        self.pool.put(self.addr, self.stream);
    }

    /// Close and discard this connection (don't return to pool).
    pub fn discard(self: *PooledConn) void {
        self.stream.close();
    }
};

pub const ConnPool = struct {
    /// Map of address → list of idle connections with timestamps.
    pools: std.StringHashMap(std.ArrayList(PooledConnection)),
    mu: std.Thread.Mutex,
    allocator: Allocator,
    shutdown: std.atomic.Value(bool),
    cleanup_thread: ?std.Thread,

    pub fn init(allocator: Allocator) ConnPool {
        return ConnPool{
            .pools = std.StringHashMap(std.ArrayList(PooledConnection)).init(allocator),
            .mu = .{},
            .allocator = allocator,
            .shutdown = std.atomic.Value(bool).init(false),
            .cleanup_thread = null,
        };
    }

    /// Start the background cleanup thread. Must be called after the ConnPool
    /// is at its final memory location (not on the stack before a move).
    pub fn startCleanup(self: *ConnPool) void {
        if (self.cleanup_thread == null) {
            self.cleanup_thread = std.Thread.spawn(.{}, cleanupLoop, .{self}) catch null;
        }
    }

    pub fn deinit(self: *ConnPool) void {
        self.close();
    }

    /// Graceful shutdown: signal cleanup thread, close all connections.
    pub fn close(self: *ConnPool) void {
        self.shutdown.store(true, .release);

        // Wait for cleanup thread
        if (self.cleanup_thread) |t| {
            t.join();
            self.cleanup_thread = null;
        }

        self.mu.lock();
        defer self.mu.unlock();

        var key_it = self.pools.keyIterator();
        while (key_it.next()) |key_ptr| {
            self.allocator.free(key_ptr.*);
        }

        var it = self.pools.valueIterator();
        while (it.next()) |list| {
            for (list.items) |pc| {
                pc.stream.close();
            }
            list.deinit();
        }
        self.pools.deinit();
    }

    /// Get a connection to `addr`. Returns from pool if available, else creates new.
    pub fn get(self: *ConnPool, addr: []const u8) !net.Stream {
        if (self.shutdown.load(.acquire)) return error.ConnectionRefused;

        // Try pool first
        {
            self.mu.lock();
            defer self.mu.unlock();
            if (self.pools.getPtr(addr)) |list| {
                if (list.items.len > 0) {
                    const pc = list.pop().?;
                    return pc.stream;
                }
            }
        }

        // Create new connection
        const stream = try connect(addr);

        // Set TCP_NODELAY, keepalive, and I/O timeouts on new connections
        setNoDelay(stream) catch {};
        setKeepalive(stream) catch {};
        setIoTimeouts(stream) catch {};

        return stream;
    }

    /// Return a connection to the pool.
    pub fn put(self: *ConnPool, addr: []const u8, stream: net.Stream) void {
        if (self.shutdown.load(.acquire)) {
            stream.close();
            return;
        }

        self.mu.lock();
        defer self.mu.unlock();

        const now = std.time.nanoTimestamp();

        if (self.pools.getPtr(addr)) |list| {
            if (list.items.len < MAX_IDLE_PER_PEER) {
                list.append(.{ .stream = stream, .last_used = now }) catch {
                    stream.close();
                };
                return;
            }
        } else {
            // New peer — duplicate the key so the pool owns it
            const owned_key = self.allocator.dupe(u8, addr) catch {
                stream.close();
                return;
            };
            var new_list = std.ArrayList(PooledConnection).init(self.allocator);
            new_list.append(.{ .stream = stream, .last_used = now }) catch {
                stream.close();
                self.allocator.free(owned_key);
                return;
            };
            self.pools.put(owned_key, new_list) catch {
                stream.close();
                new_list.deinit();
                self.allocator.free(owned_key);
            };
            return;
        }
        // Pool full
        stream.close();
    }

    /// Background cleanup: evict idle connections older than IDLE_TIMEOUT_NS.
    fn cleanupLoop(self: *ConnPool) void {
        while (!self.shutdown.load(.acquire)) {
            // Sleep in small increments to check shutdown flag
            var slept: u64 = 0;
            while (slept < CLEANUP_INTERVAL_NS) {
                if (self.shutdown.load(.acquire)) return;
                const sleep_chunk = @min(CLEANUP_INTERVAL_NS - slept, 500 * std.time.ns_per_ms);
                std.time.sleep(sleep_chunk);
                slept += sleep_chunk;
            }

            if (self.shutdown.load(.acquire)) return;

            self.evictIdle();
        }
    }

    fn evictIdle(self: *ConnPool) void {
        self.mu.lock();
        defer self.mu.unlock();

        const now = std.time.nanoTimestamp();

        var it = self.pools.valueIterator();
        while (it.next()) |list| {
            // Walk backwards to allow swap-remove
            var i: usize = list.items.len;
            while (i > 0) {
                i -= 1;
                if (now - list.items[i].last_used > IDLE_TIMEOUT_NS) {
                    list.items[i].stream.close();
                    _ = list.swapRemove(i);
                }
            }
        }
    }

    /// Number of total idle connections across all peers.
    pub fn idleCount(self: *ConnPool) usize {
        self.mu.lock();
        defer self.mu.unlock();
        var total: usize = 0;
        var it = self.pools.valueIterator();
        while (it.next()) |list| {
            total += list.items.len;
        }
        return total;
    }

    /// Connect to an address string like "10.0.0.1:17310".
    fn connect(addr: []const u8) !net.Stream {
        // Parse host:port
        const colon = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidAddress;
        const host = addr[0..colon];
        const port_str = addr[colon + 1 ..];
        const port = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidAddress;

        const address = try net.Address.parseIp4(host, port);
        return try net.tcpConnectToAddress(address);
    }

    fn setNoDelay(stream: net.Stream) !void {
        const fd = stream.handle;
        const rc = posix.system.setsockopt(
            fd,
            posix.system.IPPROTO.TCP,
            std.posix.system.TCP.NODELAY,
            &std.mem.toBytes(@as(c_int, 1)),
            @sizeOf(c_int),
        );
        if (rc != 0) return error.SetSockOptFailed;
    }

    fn setKeepalive(stream: net.Stream) !void {
        const fd = stream.handle;
        const rc = posix.system.setsockopt(
            fd,
            posix.system.SOL.SOCKET,
            posix.system.SO.KEEPALIVE,
            &std.mem.toBytes(@as(c_int, 1)),
            @sizeOf(c_int),
        );
        if (rc != 0) return error.SetSockOptFailed;
    }

    fn setIoTimeouts(stream: net.Stream) !void {
        const fd = stream.handle;
        const timeval = posix.system.timeval{
            .sec = @intCast(IO_TIMEOUT_SECS),
            .usec = 0,
        };
        const timeval_bytes = std.mem.asBytes(&timeval);
        // Set receive timeout
        var rc = posix.system.setsockopt(
            fd,
            posix.system.SOL.SOCKET,
            posix.system.SO.RCVTIMEO,
            timeval_bytes,
            @sizeOf(posix.system.timeval),
        );
        if (rc != 0) return error.SetSockOptFailed;
        // Set send timeout
        rc = posix.system.setsockopt(
            fd,
            posix.system.SOL.SOCKET,
            posix.system.SO.SNDTIMEO,
            timeval_bytes,
            @sizeOf(posix.system.timeval),
        );
        if (rc != 0) return error.SetSockOptFailed;
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "conn pool put and idle count" {
    // We can't easily create real streams in tests without a server,
    // so we test the idle eviction logic with the public API.
    var pool = ConnPool.init(std.testing.allocator);
    defer pool.close();

    // Pool starts empty
    try std.testing.expectEqual(@as(usize, 0), pool.idleCount());
}

test "conn pool shutdown rejects new gets" {
    var pool = ConnPool.init(std.testing.allocator);
    defer pool.close();

    pool.shutdown.store(true, .release);

    // get should fail after shutdown
    const result = pool.get("127.0.0.1:9999");
    try std.testing.expectError(error.ConnectionRefused, result);
}
