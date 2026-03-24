// repair.zig — Background repair system.
// Periodically compares replicas and streams missing data to repair inconsistencies.
// Leader node fetches watermarks from followers, compares them, and pushes
// missing extent data to any follower that's behind.

const std = @import("std");
const Allocator = std.mem.Allocator;
const net = std.net;
const types = @import("types.zig");
const codec = @import("codec.zig");
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const conn_pool_mod = @import("conn_pool.zig");
const crc32_mod = @import("crc32.zig");
const log = @import("log.zig");
const ExtentInfo = types.ExtentInfo;
const Packet = pkt_mod.Packet;
const ConnPool = conn_pool_mod.ConnPool;

/// Default repair interval: 60 seconds.
const REPAIR_INTERVAL_NS: u64 = 60 * std.time.ns_per_s;

/// Maximum chunk size for streaming repair reads (64 KB).
const REPAIR_READ_SIZE: u64 = 64 * 1024;

/// Maximum number of repair tasks per partition per round.
const MAX_REPAIR_TASKS: usize = 256;

/// Describes a single repair task: one extent that needs data.
pub const RepairTask = struct {
    extent_id: u64,
    local_size: u64,
    remote_size: u64,
    source_addr: []const u8,
    needs_create: bool,
};

/// Repair statistics for a single round.
pub const RepairStats = struct {
    partitions_checked: u64 = 0,
    extents_repaired: u64 = 0,
    bytes_transferred: u64 = 0,
    errors: u64 = 0,
};

/// Background repair manager. Runs in a separate thread.
pub const RepairManager = struct {
    allocator: Allocator,
    interval_ns: u64,
    running: std.atomic.Value(bool),
    thread: ?std.Thread,
    conn_pool: *ConnPool,
    last_stats: RepairStats,

    pub fn init(allocator: Allocator, conn_pool: *ConnPool) RepairManager {
        return .{
            .allocator = allocator,
            .interval_ns = REPAIR_INTERVAL_NS,
            .running = std.atomic.Value(bool).init(false),
            .thread = null,
            .conn_pool = conn_pool,
            .last_stats = .{},
        };
    }

    /// Start the repair background thread.
    pub fn start(self: *RepairManager, context: *RepairContext) void {
        if (self.running.load(.acquire)) return;
        self.running.store(true, .release);
        self.thread = std.Thread.spawn(.{}, repairLoop, .{ self, context }) catch null;
    }

    /// Stop the repair background thread.
    pub fn stop(self: *RepairManager) void {
        self.running.store(false, .release);
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    pub fn deinit(self: *RepairManager) void {
        self.stop();
    }
};

/// Context needed by the repair loop to access partitions and their replicas.
pub const RepairContext = struct {
    /// Callback to get all partition IDs where this node is leader.
    getLeaderPartitionsFn: *const fn (ctx: *anyopaque, allocator: Allocator) anyerror![]PartitionRepairInfo,
    ctx: *anyopaque,
};

/// Information about a partition needed for repair.
pub const PartitionRepairInfo = struct {
    partition_id: u64,
    local_extents: []ExtentInfo,
    replicas: [][]const u8,
};

fn repairLoop(mgr: *RepairManager, context: *RepairContext) void {
    while (mgr.running.load(.acquire)) {
        std.time.sleep(mgr.interval_ns);
        if (!mgr.running.load(.acquire)) break;

        var stats = RepairStats{};
        doRepairRound(mgr, context, &stats) catch |e| {
            log.warn("repair round failed: {}", .{e});
        };
        mgr.last_stats = stats;

        if (stats.extents_repaired > 0) {
            log.info("repair round: checked={d} repaired={d} bytes={d} errors={d}", .{
                stats.partitions_checked,
                stats.extents_repaired,
                stats.bytes_transferred,
                stats.errors,
            });
        }
    }
}

fn doRepairRound(mgr: *RepairManager, context: *RepairContext, stats: *RepairStats) !void {
    const partitions = context.getLeaderPartitionsFn(context.ctx, mgr.allocator) catch return;
    defer {
        for (partitions) |p| {
            mgr.allocator.free(p.local_extents);
        }
        mgr.allocator.free(partitions);
    }

    for (partitions) |part_info| {
        if (!mgr.running.load(.acquire)) break;
        stats.partitions_checked += 1;

        for (part_info.replicas) |replica_addr| {
            const tasks = compareWithReplica(
                mgr.allocator,
                mgr.conn_pool,
                part_info.partition_id,
                part_info.local_extents,
                replica_addr,
            ) catch continue;
            defer mgr.allocator.free(tasks);

            for (tasks) |task| {
                executeRepairTask(mgr.allocator, mgr.conn_pool, part_info.partition_id, &task) catch |e| {
                    log.warn("repair task failed for extent {d}: {}", .{ task.extent_id, e });
                    stats.errors += 1;
                    continue;
                };
                stats.extents_repaired += 1;
                stats.bytes_transferred += task.remote_size - task.local_size;
            }
        }
    }
}

/// Compare local extents with a remote replica and return repair tasks.
fn compareWithReplica(
    allocator: Allocator,
    conn_pool: *ConnPool,
    partition_id: u64,
    local_extents: []const ExtentInfo,
    replica_addr: []const u8,
) ![]RepairTask {
    // Fetch remote extent info
    const remote_extents = try getRemoteExtentInfo(allocator, conn_pool, partition_id, replica_addr);
    defer allocator.free(remote_extents);

    var tasks = std.ArrayList(RepairTask).init(allocator);
    errdefer tasks.deinit();

    // Build a map of local extents: file_id → size
    var local_map = std.AutoHashMap(u64, u64).init(allocator);
    defer local_map.deinit();
    for (local_extents) |ext| {
        if (!ext.is_deleted) {
            local_map.put(ext.file_id, ext.size) catch continue;
        }
    }

    // Compare: find extents where leader (local) has more data than follower (remote)
    // The leader pushes data to followers that are behind.
    for (local_extents) |local| {
        if (local.is_deleted) continue;
        if (tasks.items.len >= MAX_REPAIR_TASKS) break;

        // Check if the follower has this extent and if it's smaller
        var remote_size: u64 = 0;
        var needs_create = true;
        for (remote_extents) |remote| {
            if (remote.file_id == local.file_id) {
                remote_size = remote.size;
                needs_create = false;
                break;
            }
        }

        if (local.size > remote_size) {
            tasks.append(.{
                .extent_id = local.file_id,
                .local_size = local.size,
                .remote_size = remote_size,
                .source_addr = replica_addr,
                .needs_create = needs_create,
            }) catch continue;
        }
    }

    return tasks.toOwnedSlice();
}

/// Fetch extent info (watermarks) from a remote replica via OpGetAllWatermarks.
fn getRemoteExtentInfo(
    allocator: Allocator,
    conn_pool: *ConnPool,
    partition_id: u64,
    addr: []const u8,
) ![]ExtentInfo {
    // Try to get a connection from the pool
    const stream = conn_pool.get(addr) catch {
        return error.ConnectionFailed;
    };
    defer conn_pool.put(addr, stream) catch {
        stream.close();
    };

    // Build and send GetAllWatermarks request
    var req_pkt = Packet.init();
    req_pkt.opcode = proto.OP_GET_ALL_WATERMARKS;
    req_pkt.partition_id = partition_id;

    codec.encodePacket(stream.writer(), &req_pkt) catch {
        return error.SendFailed;
    };

    // Read response
    const resp_pkt = codec.decodePacket(allocator, stream.reader()) catch {
        return error.RecvFailed;
    };
    defer {
        if (resp_pkt.data_owned) {
            if (resp_pkt.allocator) |a| a.free(resp_pkt.data);
        }
    }

    if (resp_pkt.result_code != proto.OP_OK) {
        return error.RemoteError;
    }

    // Parse the response data as watermarks (binary format)
    return parseWatermarks(allocator, resp_pkt.data);
}

/// Parse binary watermark data into ExtentInfo array.
/// Each watermark entry is 20 bytes: file_id(8) + size(8) + crc(4).
fn parseWatermarks(allocator: Allocator, data: []const u8) ![]ExtentInfo {
    const ENTRY_SIZE: usize = 20;
    if (data.len == 0) return try allocator.alloc(ExtentInfo, 0);

    const count = data.len / ENTRY_SIZE;
    var result = try allocator.alloc(ExtentInfo, count);
    errdefer allocator.free(result);

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const offset = i * ENTRY_SIZE;
        result[i] = .{
            .file_id = std.mem.readInt(u64, data[offset..][0..8], .big),
            .size = std.mem.readInt(u64, data[offset + 8 ..][0..8], .big),
            .crc = std.mem.readInt(u32, data[offset + 16 ..][0..4], .big),
            .is_deleted = false,
        };
    }

    return result;
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "parseWatermarks empty data" {
    const allocator = testing.allocator;
    const result = try parseWatermarks(allocator, "");
    defer allocator.free(result);
    try testing.expectEqual(@as(usize, 0), result.len);
}

test "parseWatermarks single entry" {
    const allocator = testing.allocator;
    var data: [20]u8 = undefined;
    std.mem.writeInt(u64, data[0..8], 42, .big); // file_id
    std.mem.writeInt(u64, data[8..16], 65536, .big); // size
    std.mem.writeInt(u32, data[16..20], 0xDEADBEEF, .big); // crc

    const result = try parseWatermarks(allocator, &data);
    defer allocator.free(result);

    try testing.expectEqual(@as(usize, 1), result.len);
    try testing.expectEqual(@as(u64, 42), result[0].file_id);
    try testing.expectEqual(@as(u64, 65536), result[0].size);
    try testing.expectEqual(@as(u32, 0xDEADBEEF), result[0].crc);
    try testing.expect(!result[0].is_deleted);
}

test "parseWatermarks multiple entries" {
    const allocator = testing.allocator;
    var data: [60]u8 = undefined;

    // Entry 0
    std.mem.writeInt(u64, data[0..8], 1, .big);
    std.mem.writeInt(u64, data[8..16], 1024, .big);
    std.mem.writeInt(u32, data[16..20], 0x11, .big);

    // Entry 1
    std.mem.writeInt(u64, data[20..28], 2, .big);
    std.mem.writeInt(u64, data[28..36], 2048, .big);
    std.mem.writeInt(u32, data[36..40], 0x22, .big);

    // Entry 2
    std.mem.writeInt(u64, data[40..48], 3, .big);
    std.mem.writeInt(u64, data[48..56], 4096, .big);
    std.mem.writeInt(u32, data[56..60], 0x33, .big);

    const result = try parseWatermarks(allocator, &data);
    defer allocator.free(result);

    try testing.expectEqual(@as(usize, 3), result.len);
    try testing.expectEqual(@as(u64, 1), result[0].file_id);
    try testing.expectEqual(@as(u64, 2), result[1].file_id);
    try testing.expectEqual(@as(u64, 3), result[2].file_id);
    try testing.expectEqual(@as(u64, 4096), result[2].size);
}

test "parseWatermarks ignores trailing partial entry" {
    const allocator = testing.allocator;
    // 25 bytes = 1 full entry (20) + 5 partial bytes
    var data: [25]u8 = undefined;
    std.mem.writeInt(u64, data[0..8], 100, .big);
    std.mem.writeInt(u64, data[8..16], 200, .big);
    std.mem.writeInt(u32, data[16..20], 300, .big);
    @memset(data[20..25], 0xFF);

    const result = try parseWatermarks(allocator, &data);
    defer allocator.free(result);

    try testing.expectEqual(@as(usize, 1), result.len);
    try testing.expectEqual(@as(u64, 100), result[0].file_id);
}

test "RepairStats defaults to zero" {
    const stats = RepairStats{};
    try testing.expectEqual(@as(u64, 0), stats.partitions_checked);
    try testing.expectEqual(@as(u64, 0), stats.extents_repaired);
    try testing.expectEqual(@as(u64, 0), stats.bytes_transferred);
    try testing.expectEqual(@as(u64, 0), stats.errors);
}

test "RepairTask fields" {
    const task = RepairTask{
        .extent_id = 42,
        .local_size = 1024,
        .remote_size = 512,
        .source_addr = "10.0.0.1:17310",
        .needs_create = true,
    };
    try testing.expectEqual(@as(u64, 42), task.extent_id);
    try testing.expectEqual(@as(u64, 512), task.local_size - task.remote_size);
    try testing.expect(task.needs_create);
}

/// Execute a single repair task: stream extent data from local to remote follower.
fn executeRepairTask(
    allocator: Allocator,
    conn_pool: *ConnPool,
    partition_id: u64,
    task: *const RepairTask,
) !void {
    log.info("repair: partition {d} extent {d}: push {d} -> {d} bytes to {s}", .{
        partition_id,
        task.extent_id,
        task.remote_size,
        task.local_size,
        task.source_addr,
    });

    // Get connection to the follower
    const stream = conn_pool.get(task.source_addr) catch {
        return error.ConnectionFailed;
    };
    defer conn_pool.put(task.source_addr, stream) catch {
        stream.close();
    };

    // Send extent repair read request to the follower
    // The leader tells the follower "write this data at offset X"
    const missing = task.local_size - task.remote_size;
    var offset: u64 = task.remote_size;
    var remaining: u64 = missing;

    while (remaining > 0) {
        const chunk_size = @min(remaining, REPAIR_READ_SIZE);

        var pkt = Packet.init();
        pkt.opcode = proto.OP_EXTENT_REPAIR_READ;
        pkt.partition_id = partition_id;
        pkt.extent_id = task.extent_id;
        pkt.offset = offset;
        pkt.size = @intCast(chunk_size);

        codec.encodePacket(stream.writer(), &pkt) catch {
            return error.SendFailed;
        };

        // Read response
        const resp = codec.decodePacket(allocator, stream.reader()) catch {
            return error.RecvFailed;
        };
        defer {
            if (resp.data_owned) {
                if (resp.allocator) |a| a.free(resp.data);
            }
        }

        if (resp.result_code != proto.OP_OK) {
            return error.RemoteError;
        }

        offset += chunk_size;
        remaining -= chunk_size;
    }
}
