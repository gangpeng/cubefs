// json.zig — JSON serialization helpers for master communication.
// Handles PascalCase field names matching Go's JSON encoding.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// AdminTask envelope (matches Go's proto.AdminTask).
pub const AdminTask = struct {
    ID: []const u8 = "",
    Type: u8 = 0,
    Request: std.json.Value = .null,
    Response: std.json.Value = .null,
};

/// Disk statistics for heartbeat response.
pub const DiskStat = struct {
    Path: []const u8,
    Total: u64,
    Used: u64,
    Available: u64,
    Status: i32,
};

/// Per-partition report for heartbeat (matches Go's DataPartitionReport).
pub const DataPartitionReport = struct {
    PartitionID: u64,
    PartitionStatus: i32,
    Total: u64,
    Used: u64,
    DiskPath: []const u8 = "",
    VolName: []const u8 = "",
    IsLeader: bool = true,
    ExtentCount: u64 = 0,
    NeedCompare: bool = false,
};

// Keep the old name as an alias for backward compatibility in admin.zig
pub const PartitionReport = DataPartitionReport;

/// Heartbeat response body (matches Go's DataNodeHeartbeatResponse).
pub const HeartbeatResponse = struct {
    Total: u64 = 0,
    Used: u64 = 0,
    Available: u64 = 0,
    RemainingCapacity: u64 = 0,
    TotalPartitionSize: u64 = 0,
    CreatedPartitionCnt: u64 = 0,
    ZoneName: []const u8 = "",
    PartitionReports: []DataPartitionReport = &.{},
    DiskStats: []DiskStat = &.{},
    Status: u8 = 0,
    Result: []const u8 = "",
    BadDisks: []const []const u8 = &.{},
    StartTime: i64 = 0,
    CpuUtil: f64 = 0.0,
};

/// CreateDataPartition request parsed from AdminTask.
pub const CreatePartitionRequest = struct {
    PartitionId: u64 = 0,
    VolumeId: u64 = 0,
    PartitionSize: u64 = 0,
    Replicas: [][]const u8 = &.{},
    CreateType: u8 = 0,
};

/// Watermark info for load partition response.
pub const WatermarkInfo = struct {
    ExtentId: u64,
    Size: u64,
    Crc: u32,
};

/// Serialize a value to JSON bytes.
pub fn stringify(allocator: Allocator, value: anytype) ![]u8 {
    var list = std.ArrayList(u8).init(allocator);
    errdefer list.deinit();
    try std.json.stringify(value, .{}, list.writer());
    return list.toOwnedSlice();
}

/// Parse JSON bytes into a struct.
pub fn parse(comptime T: type, allocator: Allocator, json_bytes: []const u8) !T {
    const parsed = try std.json.parseFromSlice(T, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
    });
    return parsed.value;
}

/// Parse AdminTask from raw JSON arg bytes.
pub fn parseAdminTask(allocator: Allocator, arg: []const u8) !AdminTask {
    return parse(AdminTask, allocator, arg);
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "stringify produces valid JSON" {
    const allocator = testing.allocator;
    const report = DataPartitionReport{
        .PartitionID = 42,
        .PartitionStatus = 1,
        .Total = 1024 * 1024 * 1024,
        .Used = 512 * 1024 * 1024,
        .DiskPath = "/data0",
        .ExtentCount = 100,
        .IsLeader = true,
    };
    const json_bytes = try stringify(allocator, report);
    defer allocator.free(json_bytes);

    // Verify it contains expected fields
    try testing.expect(std.mem.indexOf(u8, json_bytes, "\"PartitionID\":42") != null);
    try testing.expect(std.mem.indexOf(u8, json_bytes, "\"IsLeader\":true") != null);
    try testing.expect(std.mem.indexOf(u8, json_bytes, "\"/data0\"") != null);
}

test "parse WatermarkInfo from JSON" {
    const allocator = testing.allocator;
    const json_str = "{\"ExtentId\":999,\"Size\":65536,\"Crc\":12345}";

    const parsed = std.json.parseFromSlice(WatermarkInfo, allocator, json_str, .{
        .ignore_unknown_fields = true,
    }) catch |e| return e;
    defer parsed.deinit();
    const wm = parsed.value;

    try testing.expectEqual(@as(u64, 999), wm.ExtentId);
    try testing.expectEqual(@as(u64, 65536), wm.Size);
    try testing.expectEqual(@as(u32, 12345), wm.Crc);
}

test "parse HeartbeatResponse with defaults" {
    const allocator = testing.allocator;
    const json_str = "{\"Total\":100,\"Used\":50,\"ZoneName\":\"zone1\"}";

    const parsed = std.json.parseFromSlice(HeartbeatResponse, allocator, json_str, .{
        .ignore_unknown_fields = true,
    }) catch |e| return e;
    defer parsed.deinit();
    const hb = parsed.value;

    try testing.expectEqual(@as(u64, 100), hb.Total);
    try testing.expectEqual(@as(u64, 50), hb.Used);
    try testing.expectEqual(@as(u64, 0), hb.Available); // default
}

test "parseAdminTask with minimal JSON" {
    const allocator = testing.allocator;
    const json_str = "{\"ID\":\"task-1\",\"Type\":2}";

    const parsed = try std.json.parseFromSlice(AdminTask, allocator, json_str, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    const task = parsed.value;

    try testing.expectEqualStrings("task-1", task.ID);
    try testing.expectEqual(@as(u8, 2), task.Type);
}

test "stringify and parse round-trip for DiskStat" {
    const allocator = testing.allocator;
    const stat = DiskStat{
        .Path = "/data1",
        .Total = 1_000_000_000,
        .Used = 500_000_000,
        .Available = 500_000_000,
        .Status = 0,
    };

    const json_bytes = try stringify(allocator, stat);
    defer allocator.free(json_bytes);

    const parsed = std.json.parseFromSlice(DiskStat, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
    }) catch |e| return e;
    defer parsed.deinit();
    const result = parsed.value;

    try testing.expectEqual(@as(u64, 1_000_000_000), result.Total);
    try testing.expectEqual(@as(u64, 500_000_000), result.Used);
    try testing.expectEqual(@as(i32, 0), result.Status);
}

test "parse CreatePartitionRequest" {
    const allocator = testing.allocator;
    const json_str = "{\"PartitionId\":1001,\"VolumeId\":5,\"PartitionSize\":137438953472}";

    const parsed = std.json.parseFromSlice(CreatePartitionRequest, allocator, json_str, .{
        .ignore_unknown_fields = true,
    }) catch |e| return e;
    defer parsed.deinit();
    const req = parsed.value;

    try testing.expectEqual(@as(u64, 1001), req.PartitionId);
    try testing.expectEqual(@as(u64, 5), req.VolumeId);
    try testing.expectEqual(@as(u64, 137438953472), req.PartitionSize);
}

test "parse ignores unknown fields" {
    const allocator = testing.allocator;
    const json_str = "{\"ExtentId\":1,\"Size\":100,\"Crc\":0,\"UnknownField\":\"value\"}";

    const parsed = std.json.parseFromSlice(WatermarkInfo, allocator, json_str, .{
        .ignore_unknown_fields = true,
    }) catch |e| return e;
    defer parsed.deinit();

    try testing.expectEqual(@as(u64, 1), parsed.value.ExtentId);
}
