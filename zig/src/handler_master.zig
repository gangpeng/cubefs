// handler_master.zig — Master command handlers: partition CRUD + heartbeat.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const Packet = pkt_mod.Packet;
const json_mod = @import("json.zig");
const space_mgr_mod = @import("space_manager.zig");
const partition_mod = @import("partition.zig");
const disk_mod = @import("disk.zig");
const raft_server_mod = @import("raft/server.zig");
const qos_mod = @import("qos.zig");
const log = @import("log.zig");
const SpaceManager = space_mgr_mod.SpaceManager;
const DataPartition = partition_mod.DataPartition;
const RaftServer = raft_server_mod.RaftServer;

/// Handle OpCreateDataPartition
pub fn handleCreateDataPartition(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    raft_server: ?*RaftServer,
    allocator: Allocator,
) Packet {
    // Parse AdminTask from arg
    if (pkt.arg.len == 0) {
        return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
    }

    const req = json_mod.parse(json_mod.CreatePartitionRequest, allocator, pkt.arg) catch {
        return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
    };

    // Select disk with most space
    const d = space_mgr.selectDisk() orelse {
        return Packet.errReply(pkt, proto.OP_DISK_NO_SPACE_ERR);
    };

    // Build partition path — must be heap-allocated to outlive this function
    var path_buf: [4096]u8 = undefined;
    const tmp_path = SpaceManager.partitionPath(d.path, req.PartitionId, &path_buf) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    const data_path = allocator.dupeZ(u8, tmp_path) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    // Create partition
    const dp = DataPartition.init(
        allocator,
        req.PartitionId,
        req.VolumeId,
        data_path,
        req.Replicas,
        d,
    ) catch {
        allocator.free(data_path);
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    if (req.PartitionSize > 0) {
        dp.setSize(req.PartitionSize);
    }

    // Start raft for this partition
    if (raft_server) |rs| {
        dp.startRaftWithServer(rs);
    } else {
        dp.startRaft();
    }

    space_mgr.addPartition(dp);
    log.info("created partition {d} on disk {s}", .{ req.PartitionId, d.path });

    return Packet.okReply(pkt);
}

/// Handle OpDeleteDataPartition
pub fn handleDeleteDataPartition(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    allocator: Allocator,
) Packet {
    const partition_id = pkt.partition_id;

    if (space_mgr.removePartition(partition_id)) |dp| {
        // Delete directory
        const path_str = dp.data_path;
        std.fs.cwd().deleteTree(path_str) catch {};
        dp.deinit();
        log.info("deleted partition {d}", .{partition_id});
        return Packet.okReply(pkt);
    }

    _ = allocator;
    return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
}

/// Handle OpLoadDataPartition — return watermarks for an existing partition
pub fn handleLoadDataPartition(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    allocator: Allocator,
) Packet {
    const dp = space_mgr.getPartition(pkt.partition_id) orelse {
        return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
    };

    const watermarks = dp.store.getAllWatermarks(allocator) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    defer allocator.free(watermarks);

    var list = std.ArrayList(json_mod.WatermarkInfo).init(allocator);
    defer list.deinit();
    for (watermarks) |w| {
        list.append(.{
            .ExtentId = w.file_id,
            .Size = w.size,
            .Crc = w.crc,
        }) catch {
            return Packet.errReply(pkt, proto.OP_ERR);
        };
    }

    const json_data = json_mod.stringify(allocator, list.items) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    var reply = Packet.okReply(pkt);
    reply.data = json_data;
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = @intCast(json_data.len);
    return reply;
}

/// Handle OpDataNodeHeartbeat
pub fn handleHeartbeat(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    allocator: Allocator,
) Packet {
    // Parse QoS config from heartbeat request if present
    if (pkt.arg.len > 0) {
        applyHeartbeatQos(pkt.arg, space_mgr, allocator);
    }

    // Build heartbeat response with disk stats and partition reports
    var total_space: u64 = 0;
    var total_used: u64 = 0;
    var total_avail: u64 = 0;

    // Disk stats
    const disk_count = space_mgr.diskCount();
    var disk_stats = std.ArrayList(json_mod.DiskStat).init(allocator);
    defer disk_stats.deinit();

    var i: usize = 0;
    while (i < disk_count) : (i += 1) {
        if (space_mgr.getDisk(i)) |d| {
            d.updateSpace() catch {};
            const t = d.getTotal();
            const u = d.getUsed();
            const a = d.getAvailable();
            total_space += t;
            total_used += u;
            total_avail += a;
            disk_stats.append(.{
                .Path = d.path,
                .Total = t,
                .Used = u,
                .Available = a,
                .Status = d.status.load(.acquire),
            }) catch {};
        }
    }

    // Partition reports
    const partitions = space_mgr.allPartitions(allocator) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    defer allocator.free(partitions);

    var reports = std.ArrayList(json_mod.PartitionReport).init(allocator);
    defer reports.deinit();

    for (partitions) |dp| {
        reports.append(.{
            .PartitionID = dp.partition_id,
            .PartitionStatus = dp.getStatus(),
            .Total = dp.getSize(),
            .Used = dp.usedSize(),
            .ExtentCount = @intCast(dp.extentCount()),
        }) catch {};
    }

    const resp = json_mod.HeartbeatResponse{
        .Total = total_space,
        .Used = total_used,
        .Available = total_avail,
        .PartitionReports = reports.items,
        .DiskStats = disk_stats.items,
    };

    const json_data = json_mod.stringify(allocator, resp) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    var reply = Packet.okReply(pkt);
    reply.data = json_data;
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = @intCast(json_data.len);
    return reply;
}

/// Parse QoS limits from heartbeat request arg and apply to all disks.
/// The master may embed QoS configuration in the heartbeat request.
/// JSON format: {"QosEnable": true, "DiskReadIops": 5000, ...}
fn applyHeartbeatQos(arg: []const u8, space_mgr: *SpaceManager, allocator: Allocator) void {
    const HeartbeatQos = struct {
        QosEnable: bool = false,
        DiskReadFlow: u64 = 0,
        DiskWriteFlow: u64 = 0,
        DiskReadIops: u64 = 0,
        DiskWriteIops: u64 = 0,
        DiskDeleteIops: u64 = 0,
        DiskReadCc: u32 = 0,
        DiskWriteCc: u32 = 0,
        DiskDeleteCc: u32 = 0,
        DecommissionDisks: ?[]const []const u8 = null,
    };

    const parsed = std.json.parseFromSlice(HeartbeatQos, allocator, arg, .{
        .ignore_unknown_fields = true,
    }) catch return;
    defer parsed.deinit();
    const hq = parsed.value;

    if (!hq.QosEnable) return;

    const config = qos_mod.QosConfig{
        .disk_read_flow = hq.DiskReadFlow,
        .disk_write_flow = hq.DiskWriteFlow,
        .disk_read_iops = hq.DiskReadIops,
        .disk_write_iops = hq.DiskWriteIops,
        .disk_delete_iops = hq.DiskDeleteIops,
        .disk_read_cc = hq.DiskReadCc,
        .disk_write_cc = hq.DiskWriteCc,
        .disk_delete_cc = hq.DiskDeleteCc,
    };

    const disk_count = space_mgr.diskCount();
    var i: usize = 0;
    while (i < disk_count) : (i += 1) {
        if (space_mgr.getDisk(i)) |d| {
            d.updateQosLimits(config);
        }
    }

    // Handle decommission disk tracking
    if (hq.DecommissionDisks) |decom_disks| {
        for (decom_disks) |decom_path| {
            var j: usize = 0;
            while (j < disk_count) : (j += 1) {
                if (space_mgr.getDisk(j)) |d| {
                    if (std.mem.eql(u8, d.path, decom_path)) {
                        d.setStatus(disk_mod.DISK_STATUS_BAD);
                        log.info("heartbeat: disk {s} marked for decommission", .{decom_path});
                    }
                }
            }
        }
    }

    log.debug("heartbeat: QoS limits applied (read_iops={d} write_iops={d})", .{
        config.disk_read_iops,
        config.disk_write_iops,
    });
}
