// config.zig — JSON configuration parser for the Zig datanode.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const DiskEntry = struct {
    path: []const u8,
    reserved_space: u64,
};

pub const Config = struct {
    local_ip: []const u8 = "0.0.0.0",
    port: u16 = 17310,
    master_addr: [][]const u8 = &.{},
    disks: [][]const u8 = &.{},
    raft_dir: []const u8 = "/tmp/cubefs/raft",
    raft_heartbeat: u16 = 17330,
    raft_replica: u16 = 17340,
    zone_name: []const u8 = "",
    log_dir: []const u8 = "/tmp/cubefs/log",
    log_level: []const u8 = "info",

    // Raft configuration
    node_id: u64 = 0,
    raft_tick_interval: u32 = 300,
    raft_election_tick: u32 = 3,
    raft_retain_logs: u64 = 20000,

    // QoS defaults (0 = unlimited)
    disk_read_flow: u64 = 0,
    disk_write_flow: u64 = 0,
    disk_read_iops: u64 = 0,
    disk_write_iops: u64 = 0,

    allocator: ?Allocator = null,

    pub fn deinit(self: *Config) void {
        _ = self;
        // JSON-parsed memory is owned by the parsed result; no-op here.
    }

    /// Parse disk entries from the config's disk strings.
    /// Format: "/path:reserved_bytes" or "/path" (reserved=0).
    pub fn parseDiskEntries(self: *const Config, allocator: Allocator) ![]DiskEntry {
        var list = std.ArrayList(DiskEntry).init(allocator);
        errdefer list.deinit();

        for (self.disks) |disk_str| {
            const entry = parseDiskString(disk_str);
            try list.append(entry);
        }
        return list.toOwnedSlice();
    }

    /// Load config from a JSON file.
    pub fn fromFile(allocator: Allocator, path: []const u8) !Config {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        const content = try file.readToEndAlloc(allocator, 1024 * 1024);
        defer allocator.free(content);

        return fromJson(allocator, content);
    }

    /// Parse config from JSON bytes. All string fields are duped into
    /// allocator-owned memory so they outlive the JSON parse tree.
    pub fn fromJson(allocator: Allocator, json_bytes: []const u8) !Config {
        const JsonConfig = struct {
            local_ip: ?[]const u8 = null,
            port: ?u16 = null,
            master_addr: ?[][]const u8 = null,
            disks: ?[][]const u8 = null,
            raft_dir: ?[]const u8 = null,
            raft_heartbeat: ?u16 = null,
            raft_replica: ?u16 = null,
            zone_name: ?[]const u8 = null,
            log_dir: ?[]const u8 = null,
            log_level: ?[]const u8 = null,
            node_id: ?u64 = null,
            raft_tick_interval: ?u32 = null,
            raft_election_tick: ?u32 = null,
            raft_retain_logs: ?u64 = null,
        };

        const parsed = try std.json.parseFromSlice(JsonConfig, allocator, json_bytes, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        const jc = parsed.value;

        // Dupe string fields so they outlive the parsed JSON
        const local_ip = try allocator.dupe(u8, jc.local_ip orelse "0.0.0.0");
        const zone_name = try allocator.dupe(u8, jc.zone_name orelse "");
        const raft_dir = try allocator.dupe(u8, jc.raft_dir orelse "/tmp/cubefs/raft");
        const log_dir = try allocator.dupe(u8, jc.log_dir orelse "/tmp/cubefs/log");
        const log_level = try allocator.dupe(u8, jc.log_level orelse "info");

        // Dupe disk strings
        const raw_disks = jc.disks orelse &.{};
        const disks = try allocator.alloc([]const u8, raw_disks.len);
        for (raw_disks, 0..) |d, i| {
            disks[i] = try allocator.dupe(u8, d);
        }

        // Dupe master_addr strings
        const raw_masters = jc.master_addr orelse &.{};
        const master_addr = try allocator.alloc([]const u8, raw_masters.len);
        for (raw_masters, 0..) |m, i| {
            master_addr[i] = try allocator.dupe(u8, m);
        }

        return Config{
            .local_ip = local_ip,
            .port = jc.port orelse 17310,
            .master_addr = master_addr,
            .disks = disks,
            .raft_dir = raft_dir,
            .raft_heartbeat = jc.raft_heartbeat orelse 17330,
            .raft_replica = jc.raft_replica orelse 17340,
            .zone_name = zone_name,
            .log_dir = log_dir,
            .log_level = log_level,
            .node_id = jc.node_id orelse 0,
            .raft_tick_interval = jc.raft_tick_interval orelse 300,
            .raft_election_tick = jc.raft_election_tick orelse 3,
            .raft_retain_logs = jc.raft_retain_logs orelse 20000,
            .allocator = allocator,
        };
    }
};

/// Parse a single disk string: "/data1:20GB" → {path="/data1", reserved=20GB}
pub fn parseDiskString(s: []const u8) DiskEntry {
    if (std.mem.indexOfScalar(u8, s, ':')) |colon| {
        const path = s[0..colon];
        const size_str = s[colon + 1 ..];
        const reserved = parseSize(size_str) orelse 0;
        return .{ .path = path, .reserved_space = reserved };
    }
    return .{ .path = s, .reserved_space = 0 };
}

/// Parse a human-readable size string (e.g. "20GB", "512MB", "1024").
fn parseSize(s: []const u8) ?u64 {
    if (s.len == 0) return null;

    // Find where the numeric part ends
    var num_end: usize = 0;
    while (num_end < s.len and (s[num_end] >= '0' and s[num_end] <= '9')) {
        num_end += 1;
    }
    if (num_end == 0) return null;

    const num = std.fmt.parseInt(u64, s[0..num_end], 10) catch return null;
    const suffix = s[num_end..];

    if (suffix.len == 0) return num;
    if (std.ascii.eqlIgnoreCase(suffix, "kb") or std.ascii.eqlIgnoreCase(suffix, "k")) return num * 1024;
    if (std.ascii.eqlIgnoreCase(suffix, "mb") or std.ascii.eqlIgnoreCase(suffix, "m")) return num * 1024 * 1024;
    if (std.ascii.eqlIgnoreCase(suffix, "gb") or std.ascii.eqlIgnoreCase(suffix, "g")) return num * 1024 * 1024 * 1024;
    if (std.ascii.eqlIgnoreCase(suffix, "tb") or std.ascii.eqlIgnoreCase(suffix, "t")) return num * 1024 * 1024 * 1024 * 1024;

    return num;
}

test "parse disk string with reserved" {
    const entry = parseDiskString("/data1:20GB");
    try std.testing.expectEqualStrings("/data1", entry.path);
    try std.testing.expectEqual(@as(u64, 20 * 1024 * 1024 * 1024), entry.reserved_space);
}

test "parse disk string without reserved" {
    const entry = parseDiskString("/data2");
    try std.testing.expectEqualStrings("/data2", entry.path);
    try std.testing.expectEqual(@as(u64, 0), entry.reserved_space);
}

test "parse size variants" {
    try std.testing.expectEqual(@as(?u64, 512 * 1024 * 1024), parseSize("512MB"));
    try std.testing.expectEqual(@as(?u64, 1024), parseSize("1024"));
    try std.testing.expectEqual(@as(?u64, 1024 * 1024 * 1024 * 1024), parseSize("1TB"));
    try std.testing.expectEqual(@as(?u64, null), parseSize(""));
}

test "fromJson with full config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"local_ip":"10.0.0.1","port":17310,"raft_dir":"/data/raft",
        \\"raft_heartbeat":17330,"raft_replica":17340,"zone_name":"z1",
        \\"log_dir":"/var/log/cubefs","log_level":"debug",
        \\"node_id":42,"raft_tick_interval":500,"raft_election_tick":5,
        \\"raft_retain_logs":50000,"disks":["/data1:10GB","/data2"],
        \\"master_addr":["10.0.0.100:17010","10.0.0.101:17010"]}
    ;

    const cfg = try Config.fromJson(allocator, json);
    defer {
        allocator.free(cfg.local_ip);
        allocator.free(cfg.zone_name);
        allocator.free(cfg.raft_dir);
        allocator.free(cfg.log_dir);
        allocator.free(cfg.log_level);
        for (cfg.disks) |d| allocator.free(d);
        allocator.free(cfg.disks);
        for (cfg.master_addr) |m| allocator.free(m);
        allocator.free(cfg.master_addr);
    }

    try std.testing.expectEqualStrings("10.0.0.1", cfg.local_ip);
    try std.testing.expectEqual(@as(u16, 17310), cfg.port);
    try std.testing.expectEqualStrings("/data/raft", cfg.raft_dir);
    try std.testing.expectEqual(@as(u16, 17330), cfg.raft_heartbeat);
    try std.testing.expectEqual(@as(u16, 17340), cfg.raft_replica);
    try std.testing.expectEqualStrings("z1", cfg.zone_name);
    try std.testing.expectEqualStrings("/var/log/cubefs", cfg.log_dir);
    try std.testing.expectEqualStrings("debug", cfg.log_level);
    try std.testing.expectEqual(@as(u64, 42), cfg.node_id);
    try std.testing.expectEqual(@as(u32, 500), cfg.raft_tick_interval);
    try std.testing.expectEqual(@as(u32, 5), cfg.raft_election_tick);
    try std.testing.expectEqual(@as(u64, 50000), cfg.raft_retain_logs);
    try std.testing.expectEqual(@as(usize, 2), cfg.disks.len);
    try std.testing.expectEqual(@as(usize, 2), cfg.master_addr.len);
}

test "fromJson with defaults" {
    const allocator = std.testing.allocator;
    const json = "{}";

    const cfg = try Config.fromJson(allocator, json);
    defer {
        allocator.free(cfg.local_ip);
        allocator.free(cfg.zone_name);
        allocator.free(cfg.raft_dir);
        allocator.free(cfg.log_dir);
        allocator.free(cfg.log_level);
        allocator.free(cfg.disks);
        allocator.free(cfg.master_addr);
    }

    try std.testing.expectEqualStrings("0.0.0.0", cfg.local_ip);
    try std.testing.expectEqual(@as(u16, 17310), cfg.port);
    try std.testing.expectEqualStrings("/tmp/cubefs/raft", cfg.raft_dir);
    try std.testing.expectEqualStrings("info", cfg.log_level);
    try std.testing.expectEqual(@as(u64, 0), cfg.node_id);
    try std.testing.expectEqual(@as(usize, 0), cfg.disks.len);
    try std.testing.expectEqual(@as(usize, 0), cfg.master_addr.len);
}

test "fromJson ignores unknown fields" {
    const allocator = std.testing.allocator;
    const json = "{\"port\":9999,\"unknown_field\":\"ignored\",\"another\":123}";

    const cfg = try Config.fromJson(allocator, json);
    defer {
        allocator.free(cfg.local_ip);
        allocator.free(cfg.zone_name);
        allocator.free(cfg.raft_dir);
        allocator.free(cfg.log_dir);
        allocator.free(cfg.log_level);
        allocator.free(cfg.disks);
        allocator.free(cfg.master_addr);
    }

    try std.testing.expectEqual(@as(u16, 9999), cfg.port);
}

test "parse size with short suffix" {
    try std.testing.expectEqual(@as(?u64, 10 * 1024), parseSize("10K"));
    try std.testing.expectEqual(@as(?u64, 5 * 1024 * 1024), parseSize("5M"));
    try std.testing.expectEqual(@as(?u64, 2 * 1024 * 1024 * 1024), parseSize("2G"));
    try std.testing.expectEqual(@as(?u64, 1024 * 1024 * 1024 * 1024), parseSize("1T"));
}

test "parse size with unknown suffix returns raw number" {
    try std.testing.expectEqual(@as(?u64, 42), parseSize("42XYZ"));
}

test "parse size with just letters returns null" {
    try std.testing.expectEqual(@as(?u64, null), parseSize("abc"));
}
