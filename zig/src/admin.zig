// admin.zig — HTTP admin server on port+1.
// Provides /status, /health, /stats, /partitions, /disks endpoints.

const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;
const space_mgr_mod = @import("space_manager.zig");
const json_mod = @import("json.zig");
const connection_mod = @import("connection.zig");
const log = @import("log.zig");
const metrics_mod = @import("metrics.zig");
const SpaceManager = space_mgr_mod.SpaceManager;

pub fn startAdminServer(
    space_mgr: *SpaceManager,
    bind_ip: []const u8,
    port: u16,
    start_time: i64,
    allocator: Allocator,
) void {
    const address = net.Address.parseIp4(bind_ip, port) catch {
        log.err("admin: invalid bind address {s}:{d}", .{ bind_ip, port });
        return;
    };

    var server = address.listen(.{
        .reuse_address = true,
    }) catch {
        log.err("admin: failed to listen on {s}:{d}", .{ bind_ip, port });
        return;
    };
    defer server.deinit();

    log.info("admin server listening on {s}:{d}", .{ bind_ip, port });

    while (true) {
        const conn = server.accept() catch continue;
        // Spawn thread per request for concurrency
        _ = std.Thread.spawn(.{}, handleAdminRequestThread, .{ conn.stream, space_mgr, start_time, allocator }) catch {
            handleAdminRequest(conn.stream, space_mgr, start_time, allocator);
        };
    }
}

fn handleAdminRequestThread(stream: net.Stream, space_mgr: *SpaceManager, start_time: i64, allocator: Allocator) void {
    handleAdminRequest(stream, space_mgr, start_time, allocator);
}

fn handleAdminRequest(stream: net.Stream, space_mgr: *SpaceManager, start_time: i64, allocator: Allocator) void {
    defer stream.close();

    // Read request (simplified HTTP parsing)
    var buf: [4096]u8 = undefined;
    const n = stream.read(&buf) catch return;
    if (n == 0) return;

    const request = buf[0..n];

    // Extract path from "GET /path HTTP/1.1"
    const path = extractPath(request) orelse "/";

    // Route
    if (std.mem.eql(u8, path, "/status")) {
        sendJsonResponse(stream, 200, handleStatus(allocator), allocator);
    } else if (std.mem.eql(u8, path, "/health")) {
        sendJsonResponse(stream, 200, handleHealth(space_mgr, allocator), allocator);
    } else if (std.mem.eql(u8, path, "/stats")) {
        sendJsonResponse(stream, 200, handleStats(space_mgr, start_time, allocator), allocator);
    } else if (std.mem.eql(u8, path, "/partitions")) {
        sendJsonResponse(stream, 200, handlePartitions(space_mgr, allocator), allocator);
    } else if (std.mem.eql(u8, path, "/disks")) {
        sendJsonResponse(stream, 200, handleDisks(space_mgr, allocator), allocator);
    } else if (std.mem.eql(u8, path, "/metrics")) {
        sendPrometheusResponse(stream, allocator);
    } else {
        // 404 for unknown routes
        const not_found = "{\"error\":\"not found\"}";
        sendJsonResponse(stream, 404, null, allocator);
        _ = stream.write(not_found) catch {};
        return;
    }
}

fn sendJsonResponse(stream: net.Stream, status_code: u16, body: ?[]u8, allocator: Allocator) void {
    const status_text = switch (status_code) {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        else => "Unknown",
    };

    const response_body = body orelse "{\"error\":\"not found\"}";
    defer if (body) |b| allocator.free(b);

    var resp_buf: [256]u8 = undefined;
    const header = std.fmt.bufPrint(&resp_buf, "HTTP/1.1 {d} {s}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{ status_code, status_text, response_body.len }) catch return;

    _ = stream.write(header) catch return;
    _ = stream.write(response_body) catch return;
}

fn extractPath(request: []const u8) ?[]const u8 {
    // Find first space after method
    const first_space = std.mem.indexOfScalar(u8, request, ' ') orelse return null;
    const rest = request[first_space + 1 ..];
    // Find second space (end of path)
    const second_space = std.mem.indexOfScalar(u8, rest, ' ') orelse return null;
    const full_path = rest[0..second_space];
    // Strip query string
    const qmark = std.mem.indexOfScalar(u8, full_path, '?');
    return if (qmark) |q| full_path[0..q] else full_path;
}

fn handleStatus(allocator: Allocator) ?[]u8 {
    const status = .{
        .engine = "zig",
        .version = "0.1.0",
        .status = "running",
    };
    return json_mod.stringify(allocator, status) catch null;
}

fn handleHealth(space_mgr: *SpaceManager, allocator: Allocator) ?[]u8 {
    const disk_count = space_mgr.diskCount();
    var all_good = true;
    var bad_count: usize = 0;

    var i: usize = 0;
    while (i < disk_count) : (i += 1) {
        if (space_mgr.getDisk(i)) |d| {
            if (!d.isGood()) {
                all_good = false;
                bad_count += 1;
            }
        }
    }

    const health_status: []const u8 = if (all_good) "healthy" else "degraded";

    const result = .{
        .status = health_status,
        .disk_count = disk_count,
        .bad_disk_count = bad_count,
        .partition_count = space_mgr.partitionCount(),
    };
    return json_mod.stringify(allocator, result) catch null;
}

fn handleStats(space_mgr: *SpaceManager, start_time: i64, allocator: Allocator) ?[]u8 {
    const now = std.time.timestamp();
    const uptime_seconds: u64 = if (now > start_time) @intCast(now - start_time) else 0;
    const active_conns = connection_mod.getMetrics().active_connections;

    const result = .{
        .uptime_seconds = uptime_seconds,
        .partition_count = space_mgr.partitionCount(),
        .disk_count = space_mgr.diskCount(),
        .active_connections = active_conns,
        .total_requests = connection_mod.getMetrics().total_requests,
    };
    return json_mod.stringify(allocator, result) catch null;
}

fn handlePartitions(space_mgr: *SpaceManager, allocator: Allocator) ?[]u8 {
    const partitions = space_mgr.allPartitions(allocator) catch return null;
    defer allocator.free(partitions);

    var list = std.ArrayList(json_mod.PartitionReport).init(allocator);
    defer list.deinit();

    for (partitions) |dp| {
        list.append(.{
            .PartitionID = dp.partition_id,
            .PartitionStatus = dp.getStatus(),
            .Total = dp.getSize(),
            .Used = dp.usedSize(),
            .DiskPath = dp.disk.path,
            .ExtentCount = @intCast(dp.extentCount()),
        }) catch continue;
    }

    return json_mod.stringify(allocator, list.items) catch null;
}

fn handleDisks(space_mgr: *SpaceManager, allocator: Allocator) ?[]u8 {
    const disk_count = space_mgr.diskCount();
    var list = std.ArrayList(json_mod.DiskStat).init(allocator);
    defer list.deinit();

    var i: usize = 0;
    while (i < disk_count) : (i += 1) {
        if (space_mgr.getDisk(i)) |d| {
            list.append(.{
                .DiskPath = d.path,
                .Total = d.getTotal(),
                .Used = d.getUsed(),
                .Available = d.getAvailable(),
                .Status = d.status.load(.acquire),
            }) catch continue;
        }
    }

    return json_mod.stringify(allocator, list.items) catch null;
}

fn sendPrometheusResponse(stream: net.Stream, allocator: Allocator) void {
    const body = metrics_mod.formatPrometheus(allocator) catch {
        const err_body = "# error generating metrics\n";
        var resp_buf: [256]u8 = undefined;
        const header = std.fmt.bufPrint(&resp_buf, "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{err_body.len}) catch return;
        _ = stream.write(header) catch return;
        _ = stream.write(err_body) catch return;
        return;
    };
    defer allocator.free(body);

    var resp_buf: [256]u8 = undefined;
    const header = std.fmt.bufPrint(&resp_buf, "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len}) catch return;
    _ = stream.write(header) catch return;
    _ = stream.write(body) catch return;
}
