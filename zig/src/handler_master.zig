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
/// The master sends an AdminTask JSON in pkt.data containing a nested
/// Request field with the CreateDataPartitionRequest.
pub fn handleCreateDataPartition(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    raft_server: ?*RaftServer,
    allocator: Allocator,
) Packet {
    // Parse AdminTask wrapper from data (not arg)
    if (pkt.data.len == 0) {
        log.warn("CreateDataPartition: empty data field", .{});
        return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
    }

    const parsed = std.json.parseFromSlice(json_mod.CreatePartitionTask, allocator, pkt.data, .{
        .ignore_unknown_fields = true,
    }) catch |e| {
        log.warn("CreateDataPartition: failed to parse AdminTask from data: {}", .{e});
        return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
    };
    defer parsed.deinit();
    const req = parsed.value.Request;

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
        0, // volume_id not used for numeric storage
        data_path,
        req.Hosts,
        d,
    ) catch {
        allocator.free(data_path);
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    if (req.PartitionSize > 0) {
        dp.setSize(@intCast(req.PartitionSize));
    }

    // Start raft for this partition
    if (raft_server) |rs| {
        dp.startRaftWithServer(rs);
    } else {
        dp.startRaft();
    }

    space_mgr.addPartition(dp);
    log.info("created partition {d} on disk {s}", .{ req.PartitionId, d.path });

    // Go DataNode returns the disk path in the response body
    var reply = Packet.okReply(pkt);
    const path_copy = allocator.dupe(u8, d.path) catch {
        return reply;
    };
    reply.data = path_copy;
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = @intCast(path_copy.len);
    return reply;
}

/// Handle OpDeleteDataPartition
/// The master sends an AdminTask JSON in pkt.data containing a nested
/// Request field with the DeleteDataPartitionRequest.
pub fn handleDeleteDataPartition(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    allocator: Allocator,
) Packet {
    // Parse AdminTask from data field
    var partition_id = pkt.partition_id;
    if (pkt.data.len > 0) {
        const parsed = std.json.parseFromSlice(json_mod.DeletePartitionTask, allocator, pkt.data, .{
            .ignore_unknown_fields = true,
        }) catch |e| {
            log.warn("DeleteDataPartition: failed to parse AdminTask: {}", .{e});
            return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
        };
        defer parsed.deinit();
        partition_id = parsed.value.Request.PartitionId;
    }

    if (space_mgr.removePartition(partition_id)) |dp| {
        const path_str = dp.data_path;
        std.fs.cwd().deleteTree(path_str) catch {};
        dp.deinit();
        log.info("deleted partition {d}", .{partition_id});
        return Packet.okReply(pkt);
    }

    return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
}

/// Handle OpLoadDataPartition — return watermarks for an existing partition.
/// The master sends an AdminTask JSON in pkt.data; the Go DataNode
/// responds with PacketOkReply immediately, then loads asynchronously.
/// For simplicity we load synchronously and return the watermarks.
pub fn handleLoadDataPartition(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    allocator: Allocator,
) Packet {
    // Parse AdminTask from data field to get partition_id
    var partition_id = pkt.partition_id;
    if (pkt.data.len > 0) {
        if (std.json.parseFromSlice(json_mod.LoadPartitionTask, allocator, pkt.data, .{
            .ignore_unknown_fields = true,
        })) |parsed| {
            if (parsed.value.Request.PartitionId != 0) {
                partition_id = parsed.value.Request.PartitionId;
            }
            parsed.deinit();
        } else |_| {
            // Fall back to packet header partition_id
        }
    }

    const dp = space_mgr.getPartition(partition_id) orelse {
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
/// The Go master sends heartbeat requests via binary protocol. The DataNode
/// should reply with okReply immediately — the actual heartbeat response data
/// is sent asynchronously via HTTP POST to /dataNode/response by the
/// heartbeat loop in server.zig.
pub fn handleHeartbeat(
    pkt: *const Packet,
    space_mgr: *SpaceManager,
    allocator: Allocator,
) Packet {
    // Parse QoS config from heartbeat request if present
    if (pkt.arg.len > 0) {
        applyHeartbeatQos(pkt.arg, space_mgr, allocator);
    }

    return Packet.okReply(pkt);
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
