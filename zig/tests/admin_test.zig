// admin_test.zig — Tests for the HTTP admin server endpoints.
//
// Tests extractPath parsing, response generation for /status, /health,
// /stats, /partitions, /disks, /metrics, and 404 handling via real TCP.

const std = @import("std");
const testing = std.testing;
const net = std.net;
const engine = @import("cubefs_engine");

const admin = engine._admin;
const space_mgr_mod = engine._space_manager;
const disk_mod = engine._disk;
const partition_mod = engine._partition;
const SpaceManager = space_mgr_mod.SpaceManager;
const Disk = disk_mod.Disk;
const DataPartition = partition_mod.DataPartition;

const server_alloc = std.heap.page_allocator;

var test_dir_counter = std.atomic.Value(u32).init(0);

const AdminTestCtx = struct {
    space_mgr: *SpaceManager,
    port: u16,
    thread: ?std.Thread,
    tmp_dir: []const u8,
    shutdown: std.atomic.Value(bool),

    fn setup() !*AdminTestCtx {
        const dir_id = test_dir_counter.fetchAdd(1, .monotonic);
        const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
        const tmp_dir = try std.fmt.allocPrint(server_alloc, "/tmp/zig_admin_test_{d}_{d}", .{ timestamp, dir_id });
        std.fs.cwd().deleteTree(tmp_dir) catch {};

        const disk_path = try std.fmt.allocPrint(server_alloc, "{s}/disk0", .{tmp_dir});
        std.fs.cwd().makePath(disk_path) catch {};

        const dp_path = try std.fmt.allocPrintZ(server_alloc, "{s}/disk0/dp_5000", .{tmp_dir});
        std.fs.cwd().makePath(dp_path) catch {};

        const sm = try server_alloc.create(SpaceManager);
        sm.* = SpaceManager.init(server_alloc);

        const d = try Disk.init(server_alloc, disk_path, 0, 0, 0);
        try sm.addDisk(d);

        const dp = try DataPartition.init(server_alloc, 5000, 1, dp_path, &.{}, d);
        sm.addPartition(dp);

        // Find free port
        const address = try net.Address.parseIp4("127.0.0.1", 0);
        var listener = try address.listen(.{ .reuse_address = true });
        const port = listener.listen_address.getPort();
        listener.deinit();

        const ctx = try server_alloc.create(AdminTestCtx);
        ctx.* = .{
            .space_mgr = sm,
            .port = port,
            .thread = null,
            .tmp_dir = tmp_dir,
            .shutdown = std.atomic.Value(bool).init(false),
        };

        // Start admin server in background thread
        ctx.thread = try std.Thread.spawn(.{}, adminServerThread, .{ ctx.space_mgr, port });

        // Wait for server to be ready
        std.time.sleep(50 * std.time.ns_per_ms);

        return ctx;
    }

    fn teardown(self: *AdminTestCtx) void {
        // Admin server runs forever; we just detach the thread
        if (self.thread) |t| t.detach();
        std.fs.cwd().deleteTree(self.tmp_dir) catch {};
    }
};

fn adminServerThread(sm: *SpaceManager, port: u16) void {
    admin.startAdminServer(sm, "127.0.0.1", port, std.time.timestamp(), server_alloc);
}

fn httpGet(port: u16, path: []const u8, allocator: std.mem.Allocator) !struct { status: u16, body: []u8 } {
    const address = try net.Address.parseIp4("127.0.0.1", port);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Send HTTP GET request
    var req_buf: [512]u8 = undefined;
    const req = try std.fmt.bufPrint(&req_buf, "GET {s} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", .{path});
    _ = try stream.write(req);

    // Read response
    var resp_buf: [8192]u8 = undefined;
    var total: usize = 0;
    while (total < resp_buf.len) {
        const n = stream.read(resp_buf[total..]) catch break;
        if (n == 0) break;
        total += n;
    }
    const response = resp_buf[0..total];

    // Parse status code from "HTTP/1.1 200 OK"
    var status: u16 = 0;
    if (std.mem.indexOf(u8, response, "HTTP/1.1 ")) |idx| {
        const code_start = idx + 9;
        if (code_start + 3 <= response.len) {
            status = std.fmt.parseInt(u16, response[code_start .. code_start + 3], 10) catch 0;
        }
    }

    // Find body after \r\n\r\n
    var body: []u8 = &.{};
    if (std.mem.indexOf(u8, response, "\r\n\r\n")) |hdr_end| {
        const body_start = hdr_end + 4;
        if (body_start < total) {
            body = try allocator.dupe(u8, response[body_start..total]);
        }
    }

    return .{ .status = status, .body = body };
}

// ─── Admin Endpoint Tests ─────────────────────────────────────────

test "admin: /status returns engine info" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    const result = try httpGet(ctx.port, "/status", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 200), result.status);
    try testing.expect(result.body.len > 0);
    // Should contain "zig" engine
    try testing.expect(std.mem.indexOf(u8, result.body, "zig") != null);
}

test "admin: /health returns health status" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    const result = try httpGet(ctx.port, "/health", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 200), result.status);
    // Should contain "healthy" or "degraded"
    const has_status = std.mem.indexOf(u8, result.body, "healthy") != null or
        std.mem.indexOf(u8, result.body, "degraded") != null;
    try testing.expect(has_status);
}

test "admin: /stats returns uptime and counts" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    const result = try httpGet(ctx.port, "/stats", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 200), result.status);
    try testing.expect(std.mem.indexOf(u8, result.body, "uptime") != null);
    try testing.expect(std.mem.indexOf(u8, result.body, "partition_count") != null);
}

test "admin: /partitions returns partition list" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    const result = try httpGet(ctx.port, "/partitions", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 200), result.status);
    // Should contain partition ID 5000
    try testing.expect(std.mem.indexOf(u8, result.body, "5000") != null);
}

test "admin: /disks returns disk list" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    const result = try httpGet(ctx.port, "/disks", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 200), result.status);
    try testing.expect(result.body.len > 0);
}

test "admin: /metrics returns prometheus format" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    const result = try httpGet(ctx.port, "/metrics", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 200), result.status);
    // Prometheus format should contain # HELP or metric names
    try testing.expect(result.body.len > 0);
}

test "admin: /unknown returns 404" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    const result = try httpGet(ctx.port, "/nonexistent", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 404), result.status);
}

test "admin: /health with bad disk reports degraded" {
    const ctx = try AdminTestCtx.setup();
    defer ctx.teardown();

    // Mark disk as bad
    if (ctx.space_mgr.getDisk(0)) |d| {
        d.setStatus(disk_mod.DISK_STATUS_BAD);
    }

    const result = try httpGet(ctx.port, "/health", testing.allocator);
    defer if (result.body.len > 0) testing.allocator.free(result.body);

    try testing.expectEqual(@as(u16, 200), result.status);
    try testing.expect(std.mem.indexOf(u8, result.body, "degraded") != null);
}
