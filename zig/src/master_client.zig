// master_client.zig — HTTP registration + heartbeat POST to CubeFS master.

const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;
const log = @import("log.zig");
const json_mod = @import("json.zig");

const MAX_RETRIES: usize = 3;
const INITIAL_BACKOFF_MS: u64 = 100;

pub const MasterClient = struct {
    master_addrs: []const []const u8,
    local_ip: []const u8,
    port: u16,
    zone_name: []const u8,
    raft_heartbeat: u16,
    raft_replica: u16,
    allocator: Allocator,

    pub fn init(
        allocator: Allocator,
        master_addrs: []const []const u8,
        local_ip: []const u8,
        port: u16,
        zone_name: []const u8,
        raft_heartbeat: u16,
        raft_replica: u16,
    ) MasterClient {
        return .{
            .master_addrs = master_addrs,
            .local_ip = local_ip,
            .port = port,
            .zone_name = zone_name,
            .raft_heartbeat = raft_heartbeat,
            .raft_replica = raft_replica,
            .allocator = allocator,
        };
    }

    /// Register this datanode with the master.
    /// GET /dataNode/add?addr=ip:port&zoneName=...
    pub fn register(self: *MasterClient) !void {
        var path_buf: [1024]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "/dataNode/add?addr={s}:{d}&zoneName={s}&raftHeartbeatPort={d}&raftReplicaPort={d}", .{
            self.local_ip,
            self.port,
            self.zone_name,
            self.raft_heartbeat,
            self.raft_replica,
        }) catch return error.Overflow;

        for (self.master_addrs) |master| {
            if (self.httpGetWithRetry(master, path)) |resp| {
                self.allocator.free(resp);
                log.info("registered with master {s}", .{master});
                return;
            } else |_| {
                log.warn("failed to register with master {s}", .{master});
            }
        }
        return error.ConnectionRefused;
    }

    /// Send a heartbeat response body to the master.
    /// POST /dataNode/response with AdminTask envelope.
    pub fn sendHeartbeat(self: *MasterClient, body: []const u8) !void {
        for (self.master_addrs) |master| {
            if (self.httpPostWithRetry(master, "/dataNode/response", body)) |resp| {
                self.allocator.free(resp);
                return;
            } else |_| {
                continue;
            }
        }
        return error.ConnectionRefused;
    }

    /// Send a task response to the master.
    /// POST /dataNode/response
    pub fn sendTaskResponse(self: *MasterClient, body: []const u8) !void {
        for (self.master_addrs) |master| {
            if (self.httpPostWithRetry(master, "/dataNode/response", body)) |resp| {
                self.allocator.free(resp);
                return;
            } else |_| {
                continue;
            }
        }
        return error.ConnectionRefused;
    }

    fn httpGetWithRetry(self: *MasterClient, addr: []const u8, path: []const u8) ![]u8 {
        return self.httpRequestWithRetry(addr, "GET", path, "");
    }

    fn httpPostWithRetry(self: *MasterClient, addr: []const u8, path: []const u8, body: []const u8) ![]u8 {
        return self.httpRequestWithRetry(addr, "POST", path, body);
    }

    /// HTTP request with exponential backoff retry (3 retries, 100ms/200ms/400ms).
    fn httpRequestWithRetry(self: *MasterClient, addr: []const u8, method: []const u8, path: []const u8, body: []const u8) ![]u8 {
        var backoff_ms: u64 = INITIAL_BACKOFF_MS;
        var last_err: anyerror = error.ConnectionRefused;

        for (0..MAX_RETRIES) |attempt| {
            if (self.httpRequest(addr, method, path, body)) |resp| {
                return resp;
            } else |err| {
                last_err = err;
                if (attempt + 1 < MAX_RETRIES) {
                    log.warn("HTTP {s} {s} attempt {d} failed, retrying in {d}ms", .{ method, path, attempt + 1, backoff_ms });
                    std.time.sleep(backoff_ms * std.time.ns_per_ms);
                    backoff_ms *= 2;
                }
            }
        }
        return last_err;
    }

    /// Simple HTTP/1.1 request over raw TCP.
    fn httpRequest(self: *MasterClient, addr: []const u8, method: []const u8, path: []const u8, body: []const u8) ![]u8 {
        // Parse host:port
        const colon = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidAddress;
        const host = addr[0..colon];
        const port_str = addr[colon + 1 ..];
        const port = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidAddress;

        const address = try net.Address.parseIp4(host, port);
        const stream = try net.tcpConnectToAddress(address);
        defer stream.close();

        // Build request
        var req_buf: [4096]u8 = undefined;
        const req = std.fmt.bufPrint(&req_buf, "{s} {s} HTTP/1.1\r\nHost: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{
            method,
            path,
            addr,
            body.len,
        }) catch return error.Overflow;

        _ = stream.write(req) catch return error.BrokenPipe;
        if (body.len > 0) {
            _ = stream.write(body) catch return error.BrokenPipe;
        }

        // Read response (simplified: just read up to 8KB)
        var resp_buf: [8192]u8 = undefined;
        const n = stream.read(&resp_buf) catch return error.BrokenPipe;
        if (n == 0) return error.ConnectionClosed;

        const raw_resp = resp_buf[0..n];

        // Parse HTTP status code
        const status_code = parseStatusCode(raw_resp);
        if (status_code >= 400) {
            log.warn("HTTP {d} from {s}{s}", .{ status_code, addr, path });
            return error.HttpError;
        }

        // Extract body after \r\n\r\n header separator
        const resp_body = extractBody(raw_resp);

        // Return the response body (allocator-owned copy)
        return try self.allocator.dupe(u8, resp_body);
    }

    /// Parse HTTP status code from response line "HTTP/1.1 200 OK\r\n..."
    fn parseStatusCode(response: []const u8) u16 {
        // Find "HTTP/1.x NNN"
        if (response.len < 12) return 0;
        const space1 = std.mem.indexOfScalar(u8, response, ' ') orelse return 0;
        if (space1 + 4 > response.len) return 0;
        const code_str = response[space1 + 1 .. space1 + 4];
        return std.fmt.parseInt(u16, code_str, 10) catch 0;
    }

    /// Extract body from HTTP response (everything after \r\n\r\n).
    fn extractBody(response: []const u8) []const u8 {
        const separator = "\r\n\r\n";
        if (std.mem.indexOf(u8, response, separator)) |pos| {
            return response[pos + separator.len ..];
        }
        return response;
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "parse HTTP status code" {
    try std.testing.expectEqual(@as(u16, 200), MasterClient.parseStatusCode("HTTP/1.1 200 OK\r\n"));
    try std.testing.expectEqual(@as(u16, 404), MasterClient.parseStatusCode("HTTP/1.1 404 Not Found\r\n"));
    try std.testing.expectEqual(@as(u16, 500), MasterClient.parseStatusCode("HTTP/1.1 500 Internal Server Error\r\n"));
    try std.testing.expectEqual(@as(u16, 0), MasterClient.parseStatusCode(""));
}

test "extract HTTP body" {
    const resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\"}";
    const body = MasterClient.extractBody(resp);
    try std.testing.expectEqualStrings("{\"status\":\"ok\"}", body);
}

test "extract HTTP body no separator" {
    const resp = "raw data";
    const body = MasterClient.extractBody(resp);
    try std.testing.expectEqualStrings("raw data", body);
}
