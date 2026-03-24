// partition_meta.zig — Partition metadata persistence.
// Stores partition metadata as a JSON META file in the partition directory.
// Uses atomic write (write-to-temp + fsync + rename) for crash safety.

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("log.zig");

const META_FILENAME = "META";
const TEMP_META_FILENAME = ".meta";

/// Partition metadata — serialized to/from JSON in the META file.
pub const PartitionMetadata = struct {
    volume_id: u64 = 0,
    partition_id: u64 = 0,
    partition_size: u64 = 0,
    partition_type: i32 = 0,
    create_time: []const u8 = "",
    peers: []const PeerInfo = &.{},
    hosts: []const []const u8 = &.{},
    last_truncate_id: u64 = 0,
    replica_num: u32 = 0,
    stop_recover: bool = false,
    applied_id: u64 = 0,
    disk_err_cnt: u64 = 0,
    is_repairing: bool = false,
};

pub const PeerInfo = struct {
    id: u64 = 0,
    addr: []const u8 = "",
    heartbeat_port: u16 = 0,
    replica_port: u16 = 0,
};

/// Persist metadata to `<dir>/META` using atomic write.
/// Writes to a temporary file, fsyncs, then renames.
pub fn persistMetadata(dir_path: []const u8, meta: *const PartitionMetadata, allocator: Allocator) !void {
    // Serialize to JSON
    const json_bytes = try serialize(meta, allocator);
    defer allocator.free(json_bytes);

    // Build file paths
    var path_buf: [4096]u8 = undefined;
    const temp_path = std.fmt.bufPrintZ(&path_buf, "{s}/{s}", .{ dir_path, TEMP_META_FILENAME }) catch return error.PathTooLong;

    var meta_path_buf: [4096]u8 = undefined;
    const meta_path = std.fmt.bufPrintZ(&meta_path_buf, "{s}/{s}", .{ dir_path, META_FILENAME }) catch return error.PathTooLong;

    // Write to temporary file
    const temp_file = std.fs.cwd().createFileZ(temp_path, .{ .truncate = true }) catch {
        return error.IoError;
    };

    temp_file.writeAll(json_bytes) catch {
        temp_file.close();
        std.fs.cwd().deleteFileZ(temp_path) catch {};
        return error.IoError;
    };

    // Fsync
    temp_file.sync() catch {
        temp_file.close();
        std.fs.cwd().deleteFileZ(temp_path) catch {};
        return error.IoError;
    };
    temp_file.close();

    // Atomic rename
    std.fs.cwd().renameZ(temp_path, meta_path) catch {
        std.fs.cwd().deleteFileZ(temp_path) catch {};
        return error.IoError;
    };
}

/// Load metadata from `<dir>/META`.
pub fn loadMetadata(dir_path: []const u8, allocator: Allocator) !PartitionMetadata {
    var path_buf: [4096]u8 = undefined;
    const meta_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, META_FILENAME }) catch return error.PathTooLong;

    const file = std.fs.cwd().openFile(meta_path, .{}) catch {
        return error.MetaNotFound;
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, 1024 * 1024) catch {
        return error.IoError;
    };
    defer allocator.free(content);

    return deserialize(content, allocator);
}

/// Serialize metadata to JSON bytes.
fn serialize(meta: *const PartitionMetadata, allocator: Allocator) ![]u8 {
    const JsonPeer = struct {
        ID: u64,
        Addr: []const u8,
        HeartbeatPort: u16,
        ReplicaPort: u16,
    };

    // Use a JSON-compatible struct for serialization
    const JsonMeta = struct {
        VolumeID: u64,
        PartitionID: u64,
        PartitionSize: u64,
        PartitionType: i32,
        CreateTime: []const u8,
        Peers: []const JsonPeer,
        Hosts: []const []const u8,
        LastTruncateID: u64,
        ReplicaNum: u32,
        StopRecover: bool,
        ApplyID: u64,
        DiskErrCnt: u64,
        IsRepairing: bool,
    };

    // Convert PeerInfo to JsonPeer
    const json_peers = try allocator.alloc(JsonPeer, meta.peers.len);
    defer allocator.free(json_peers);

    for (meta.peers, 0..) |p, i| {
        json_peers[i] = .{
            .ID = p.id,
            .Addr = p.addr,
            .HeartbeatPort = p.heartbeat_port,
            .ReplicaPort = p.replica_port,
        };
    }

    const json_meta = JsonMeta{
        .VolumeID = meta.volume_id,
        .PartitionID = meta.partition_id,
        .PartitionSize = meta.partition_size,
        .PartitionType = meta.partition_type,
        .CreateTime = meta.create_time,
        .Peers = json_peers,
        .Hosts = meta.hosts,
        .LastTruncateID = meta.last_truncate_id,
        .ReplicaNum = meta.replica_num,
        .StopRecover = meta.stop_recover,
        .ApplyID = meta.applied_id,
        .DiskErrCnt = meta.disk_err_cnt,
        .IsRepairing = meta.is_repairing,
    };

    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    std.json.stringify(json_meta, .{}, buf.writer()) catch return error.SerializeError;

    return buf.toOwnedSlice();
}

/// Deserialize metadata from JSON bytes.
fn deserialize(content: []const u8, allocator: Allocator) !PartitionMetadata {
    const JsonMeta = struct {
        VolumeID: ?u64 = null,
        PartitionID: ?u64 = null,
        PartitionSize: ?u64 = null,
        PartitionType: ?i32 = null,
        CreateTime: ?[]const u8 = null,
        LastTruncateID: ?u64 = null,
        ReplicaNum: ?u32 = null,
        StopRecover: ?bool = null,
        ApplyID: ?u64 = null,
        DiskErrCnt: ?u64 = null,
        IsRepairing: ?bool = null,
    };

    const parsed = std.json.parseFromSlice(JsonMeta, allocator, content, .{
        .ignore_unknown_fields = true,
    }) catch return error.DeserializeError;
    defer parsed.deinit();
    const jm = parsed.value;

    return PartitionMetadata{
        .volume_id = jm.VolumeID orelse 0,
        .partition_id = jm.PartitionID orelse 0,
        .partition_size = jm.PartitionSize orelse 0,
        .partition_type = jm.PartitionType orelse 0,
        .create_time = "",
        .last_truncate_id = jm.LastTruncateID orelse 0,
        .replica_num = jm.ReplicaNum orelse 0,
        .stop_recover = jm.StopRecover orelse false,
        .applied_id = jm.ApplyID orelse 0,
        .disk_err_cnt = jm.DiskErrCnt orelse 0,
        .is_repairing = jm.IsRepairing orelse false,
    };
}

pub const Error = error{
    PathTooLong,
    IoError,
    MetaNotFound,
    SerializeError,
    DeserializeError,
};

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "persist and load metadata round-trip" {
    const alloc = testing.allocator;

    // Create temp directory
    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const dir_path = try std.fmt.allocPrint(alloc, "/tmp/zig_meta_test_{d}", .{timestamp});
    defer alloc.free(dir_path);
    defer std.fs.cwd().deleteTree(dir_path) catch {};

    std.fs.cwd().makePath(dir_path) catch {};

    const meta = PartitionMetadata{
        .volume_id = 42,
        .partition_id = 100,
        .partition_size = 128 * 1024 * 1024 * 1024, // 128 GB
        .partition_type = 0,
        .replica_num = 3,
        .applied_id = 999,
        .last_truncate_id = 500,
        .disk_err_cnt = 2,
        .is_repairing = true,
    };

    try persistMetadata(dir_path, &meta, alloc);

    const loaded = try loadMetadata(dir_path, alloc);

    try testing.expectEqual(@as(u64, 42), loaded.volume_id);
    try testing.expectEqual(@as(u64, 100), loaded.partition_id);
    try testing.expectEqual(@as(u64, 128 * 1024 * 1024 * 1024), loaded.partition_size);
    try testing.expectEqual(@as(u32, 3), loaded.replica_num);
    try testing.expectEqual(@as(u64, 999), loaded.applied_id);
    try testing.expectEqual(@as(u64, 500), loaded.last_truncate_id);
    try testing.expectEqual(@as(u64, 2), loaded.disk_err_cnt);
    try testing.expect(loaded.is_repairing);
}

test "load metadata from nonexistent directory returns error" {
    const alloc = testing.allocator;
    const result = loadMetadata("/tmp/nonexistent_meta_dir_zig_test_12345", alloc);
    try testing.expectError(error.MetaNotFound, result);
}

test "persist overwrites existing metadata" {
    const alloc = testing.allocator;

    const timestamp = @as(u64, @intCast(std.time.nanoTimestamp()));
    const dir_path = try std.fmt.allocPrint(alloc, "/tmp/zig_meta_overwrite_{d}", .{timestamp});
    defer alloc.free(dir_path);
    defer std.fs.cwd().deleteTree(dir_path) catch {};

    std.fs.cwd().makePath(dir_path) catch {};

    // Write first version
    const meta1 = PartitionMetadata{ .partition_id = 100, .applied_id = 10 };
    try persistMetadata(dir_path, &meta1, alloc);

    // Write second version
    const meta2 = PartitionMetadata{ .partition_id = 100, .applied_id = 20 };
    try persistMetadata(dir_path, &meta2, alloc);

    // Load should return second version
    const loaded = try loadMetadata(dir_path, alloc);
    try testing.expectEqual(@as(u64, 20), loaded.applied_id);
}
