// transport.zig — TCP transport for Raft RPCs.
//
// Uses the existing ConnPool for connection management. Handles sending
// and receiving Raft messages over TCP connections.

const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;
const raft_msgs = @import("messages.zig");
const Message = raft_msgs.Message;
const log = @import("../log.zig");

/// Handler interface for incoming Raft messages.
pub const MessageHandler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        handleMessage: *const fn (ctx: *anyopaque, partition_id: u64, data: []const u8, allocator: Allocator) void,
    };

    pub fn handleMessage(self: *const MessageHandler, partition_id: u64, data: []const u8, allocator: Allocator) void {
        self.vtable.handleMessage(self.ptr, partition_id, data, allocator);
    }
};

/// TCP transport for Raft RPC communication.
pub const RaftTransport = struct {
    listen_thread: ?std.Thread,
    running: std.atomic.Value(bool),
    allocator: Allocator,
    handler: ?MessageHandler,

    pub fn init(allocator: Allocator) RaftTransport {
        return .{
            .listen_thread = null,
            .running = std.atomic.Value(bool).init(false),
            .allocator = allocator,
            .handler = null,
        };
    }

    /// Set the message handler.
    pub fn setHandler(self: *RaftTransport, handler: MessageHandler) void {
        self.handler = handler;
    }

    /// Start the TCP listener for incoming Raft messages.
    pub fn startListener(self: *RaftTransport, bind_addr: []const u8, port: u16) !void {
        if (self.running.load(.acquire)) return;
        self.running.store(true, .release);

        const address = try net.Address.parseIp4(bind_addr, port);
        const server = try address.listen(.{
            .reuse_address = true,
        });

        // Move server to heap so the thread can own it
        const server_ptr = try self.allocator.create(net.Server);
        server_ptr.* = server;

        self.listen_thread = std.Thread.spawn(.{}, listenLoop, .{ self, server_ptr }) catch {
            self.running.store(false, .release);
            server_ptr.deinit();
            self.allocator.destroy(server_ptr);
            return error.ThreadSpawnFailed;
        };
    }

    /// Stop the transport listener.
    pub fn stop(self: *RaftTransport) void {
        self.running.store(false, .release);

        if (self.listen_thread) |t| {
            t.join();
            self.listen_thread = null;
        }
    }

    pub fn deinit(self: *RaftTransport) void {
        self.stop();
    }

    /// Send a raw message to a remote address (addr:port format).
    pub fn send(self: *RaftTransport, addr: []const u8, port: u16, msg_bytes: []const u8) !void {
        if (!self.running.load(.acquire)) return;

        const address = net.Address.parseIp4(addr, port) catch return;
        const stream = net.tcpConnectToAddress(address) catch return;
        defer stream.close();

        // Write length-prefixed message
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(msg_bytes.len), .big);
        stream.writeAll(&len_buf) catch return;
        stream.writeAll(msg_bytes) catch return;
    }

    /// Send an encoded Raft Message to a peer.
    pub fn sendMessage(self: *RaftTransport, addr: []const u8, port: u16, msg: *const Message) !void {
        const encoded = try raft_msgs.encode(self.allocator, msg);
        defer self.allocator.free(encoded);
        try self.send(addr, port, encoded);
    }

    fn listenLoop(self: *RaftTransport, server: *net.Server) void {
        defer {
            server.deinit();
            self.allocator.destroy(server);
        }

        while (self.running.load(.acquire)) {
            const conn = server.accept() catch |e| {
                if (!self.running.load(.acquire)) break;
                log.debug("raft transport: accept error: {}", .{e});
                continue;
            };

            // Handle in a new thread
            _ = std.Thread.spawn(.{}, handleIncoming, .{ self, conn.stream }) catch {
                conn.stream.close();
            };
        }
    }

    fn handleIncoming(self: *RaftTransport, stream: net.Stream) void {
        defer stream.close();

        // Read length prefix
        var len_buf: [4]u8 = undefined;
        stream.reader().readNoEof(&len_buf) catch return;
        const msg_len = std.mem.readInt(u32, &len_buf, .big);

        if (msg_len == 0 or msg_len > 64 * 1024 * 1024) return;

        // Read message body
        const buf = self.allocator.alloc(u8, msg_len) catch return;
        defer self.allocator.free(buf);

        stream.reader().readNoEof(buf) catch return;

        // Parse header to get partition_id, then dispatch
        if (buf.len >= raft_msgs.HEADER_SIZE) {
            const partition_id = std.mem.readInt(u64, buf[33..41], .big);
            if (self.handler) |handler| {
                handler.handleMessage(partition_id, buf, self.allocator);
            }
        }
    }
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "RaftTransport init" {
    var transport = RaftTransport.init(testing.allocator);
    defer transport.deinit();

    try testing.expect(!transport.running.load(.acquire));
    try testing.expect(transport.listen_thread == null);
}
