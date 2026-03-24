// handler.zig — Opcode dispatcher: maps opcodes to handlers.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const Packet = pkt_mod.Packet;
const space_mgr_mod = @import("space_manager.zig");
const partition_mod = @import("partition.zig");
const replication_mod = @import("replication.zig");
const master_client_mod = @import("master_client.zig");
const handler_io = @import("handler_io.zig");
const handler_master = @import("handler_master.zig");
const raft_server_mod = @import("raft/server.zig");
const log = @import("log.zig");
const SpaceManager = space_mgr_mod.SpaceManager;
const DataPartition = partition_mod.DataPartition;
const ReplicationPipeline = replication_mod.ReplicationPipeline;
const MasterClient = master_client_mod.MasterClient;
const RaftServer = raft_server_mod.RaftServer;

pub const ServerInner = struct {
    space_mgr: *SpaceManager,
    master_client: *MasterClient,
    replication: *ReplicationPipeline,
    raft_server: ?*RaftServer,
    local_addr: []const u8,
    port: u16,
    allocator: Allocator,

    pub fn init(
        allocator: Allocator,
        space_mgr: *SpaceManager,
        master_client: *MasterClient,
        replication: *ReplicationPipeline,
        raft_server: ?*RaftServer,
        local_addr: []const u8,
        port: u16,
    ) ServerInner {
        return .{
            .space_mgr = space_mgr,
            .master_client = master_client,
            .replication = replication,
            .raft_server = raft_server,
            .local_addr = local_addr,
            .port = port,
            .allocator = allocator,
        };
    }

    /// Dispatch a packet to the appropriate handler. Returns the response packet.
    pub fn dispatch(self: *ServerInner, pkt: *const Packet) Packet {
        return switch (pkt.opcode) {
            // ── I/O operations ────────────────────────────────────
            proto.OP_CREATE_EXTENT => self.handleOpWithReplication(pkt, handler_io.handleCreateExtent),
            proto.OP_WRITE, proto.OP_SYNC_WRITE => self.handleWriteWithReplication(pkt),
            proto.OP_RANDOM_WRITE,
            proto.OP_SYNC_RANDOM_WRITE,
            proto.OP_RANDOM_WRITE_APPEND,
            proto.OP_SYNC_RANDOM_WRITE_APPEND,
            proto.OP_RANDOM_WRITE_VER,
            proto.OP_SYNC_RANDOM_WRITE_VER,
            proto.OP_TRY_WRITE_APPEND,
            proto.OP_SYNC_TRY_WRITE_APPEND,
            => self.handleWriteWithReplication(pkt),
            proto.OP_READ => self.withPartition(pkt, handler_io.handleRead),
            proto.OP_STREAM_READ, proto.OP_STREAM_FOLLOWER_READ => self.withPartition(pkt, handler_io.handleStreamRead),
            proto.OP_MARK_DELETE => self.handleOpWithReplication(pkt, handler_io.handleMarkDelete),
            proto.OP_GET_ALL_WATERMARKS => self.withPartition(pkt, handler_io.handleGetWatermarks),
            proto.OP_GET_APPLIED_ID => self.withPartition(pkt, handler_io.handleGetAppliedId),
            proto.OP_GET_PARTITION_SIZE => self.withPartition(pkt, handler_io.handleGetPartitionSize),
            proto.OP_GET_MAX_EXTENT_ID_AND_PARTITION_SIZE => self.withPartition(pkt, handler_io.handleGetMaxExtentIdAndPartitionSize),
            proto.OP_BROADCAST_MIN_APPLIED_ID => self.withPartition(pkt, handler_io.handleBroadcastMinAppliedId),
            proto.OP_READ_TINY_DELETE_RECORD => self.withPartition(pkt, handler_io.handleReadTinyDeleteRecord),

            // ── Master commands ───────────────────────────────────
            proto.OP_CREATE_DATA_PARTITION => handler_master.handleCreateDataPartition(pkt, self.space_mgr, self.raft_server, self.allocator),
            proto.OP_DELETE_DATA_PARTITION => handler_master.handleDeleteDataPartition(pkt, self.space_mgr, self.allocator),
            proto.OP_LOAD_DATA_PARTITION => handler_master.handleLoadDataPartition(pkt, self.space_mgr, self.allocator),
            proto.OP_DATA_NODE_HEARTBEAT => handler_master.handleHeartbeat(pkt, self.space_mgr, self.allocator),

            // ── Raft member operations ────────────────────────────
            proto.OP_ADD_DATA_PARTITION_RAFT_MEMBER => self.handleRaftMemberAdd(pkt),
            proto.OP_REMOVE_DATA_PARTITION_RAFT_MEMBER => self.handleRaftMemberRemove(pkt),
            proto.OP_DATA_PARTITION_TRY_TO_LEADER => self.handleTryToLeader(pkt),

            // ── Repair read operations ────────────────────────────
            proto.OP_EXTENT_REPAIR_READ => self.withPartition(pkt, handler_io.handleExtentRepairRead),
            proto.OP_TINY_EXTENT_REPAIR_READ => self.withPartition(pkt, handler_io.handleTinyExtentRepairRead),
            proto.OP_SNAPSHOT_EXTENT_REPAIR_READ => self.withPartition(pkt, handler_io.handleSnapshotExtentRepairRead),

            // ── Backup operations ────────────────────────────────
            proto.OP_BACKUP_READ => self.withPartition(pkt, handler_io.handleBackupRead),
            proto.OP_BACKUP_WRITE => self.handleWriteWithReplication(pkt),

            // ── Batch lock/unlock ────────────────────────────────
            proto.OP_BATCH_LOCK_NORMAL_EXTENT => self.withPartition(pkt, handler_io.handleBatchLockExtent),
            proto.OP_BATCH_UNLOCK_NORMAL_EXTENT => self.withPartition(pkt, handler_io.handleBatchUnlockExtent),

            // ── Partition lifecycle ──────────────────────────────
            proto.OP_DECOMMISSION_DATA_PARTITION => self.withPartition(pkt, handler_io.handleDecommission),
            proto.OP_NOTIFY_REPLICAS_TO_REPAIR => self.withPartition(pkt, handler_io.handleNotifyReplicasToRepair),
            proto.OP_STOP_DATA_PARTITION_REPAIR => self.withPartition(pkt, handler_io.handleStopRepair),

            // ── QoS ──────────────────────────────────────────────
            proto.OP_QOS => self.handleQos(pkt),

            else => blk: {
                log.warn("unknown opcode: 0x{x:0>2} ({s})", .{ pkt.opcode, proto.opcodeName(pkt.opcode) });
                break :blk Packet.errReply(pkt, proto.OP_ERR);
            },
        };
    }

    /// Look up the partition and delegate to the handler. Returns error reply if partition not found.
    fn withPartition(
        self: *ServerInner,
        pkt: *const Packet,
        handler: fn (*const Packet, *DataPartition, Allocator) Packet,
    ) Packet {
        const dp = self.space_mgr.getPartition(pkt.partition_id) orelse {
            return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
        };
        return handler(pkt, dp, self.allocator);
    }

    /// Handle write opcodes with optional replication.
    fn handleWriteWithReplication(self: *ServerInner, pkt: *const Packet) Packet {
        const dp = self.space_mgr.getPartition(pkt.partition_id) orelse {
            return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
        };

        // Execute local write
        var local_result = if (proto.isRandomWrite(pkt.opcode))
            handler_io.handleRandomWrite(pkt, dp, self.allocator)
        else
            handler_io.handleWrite(pkt, dp, self.allocator);

        // Forward to followers if needed
        if (pkt.isForward()) {
            const repl_result = self.replication.replicateOp(pkt, &local_result, self.allocator);

            // If local write succeeded but replication had failures, log it
            // but still return the local result (leader succeeds)
            if (!repl_result.isOk() and local_result.result_code == proto.OP_OK) {
                log.warn("replication partial failure for req_id={d}: {d}/{d} followers failed", .{
                    pkt.req_id,
                    repl_result.failure_count,
                    repl_result.failure_count + repl_result.success_count,
                });
            }
        }

        return local_result;
    }

    /// Handle any opcode with optional replication (create extent, mark delete, etc.).
    /// Executes the local handler, then forwards to followers if needed.
    fn handleOpWithReplication(
        self: *ServerInner,
        pkt: *const Packet,
        handler: fn (*const Packet, *DataPartition, Allocator) Packet,
    ) Packet {
        const dp = self.space_mgr.getPartition(pkt.partition_id) orelse {
            return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
        };

        var local_result = handler(pkt, dp, self.allocator);

        // Forward to followers if needed
        if (pkt.isForward()) {
            const repl_result = self.replication.replicateOp(pkt, &local_result, self.allocator);

            if (!repl_result.isOk() and local_result.result_code == proto.OP_OK) {
                log.warn("replication partial failure for req_id={d} op=0x{x:0>2}: {d}/{d} followers failed", .{
                    pkt.req_id,
                    pkt.opcode,
                    repl_result.failure_count,
                    repl_result.failure_count + repl_result.success_count,
                });
            }
        }

        return local_result;
    }

    /// Scan disk directories for existing partitions and load them.
    pub fn loadPartitions(self: *ServerInner) void {
        const disk_count = self.space_mgr.diskCount();
        var i: usize = 0;
        while (i < disk_count) : (i += 1) {
            const d = self.space_mgr.getDisk(i) orelse continue;
            self.loadPartitionsFromDisk(d);
        }
    }

    fn loadPartitionsFromDisk(self: *ServerInner, d: *@import("disk.zig").Disk) void {
        var dir = std.fs.cwd().openDir(d.path, .{ .iterate = true }) catch return;
        defer dir.close();

        var iter = dir.iterate();
        while (iter.next() catch null) |entry| {
            if (entry.kind != .directory) continue;

            // Match "datapartition_<id>"
            const prefix = "datapartition_";
            if (!std.mem.startsWith(u8, entry.name, prefix)) continue;
            const id_str = entry.name[prefix.len..];
            const partition_id = std.fmt.parseInt(u64, id_str, 10) catch continue;

            // Already loaded?
            if (self.space_mgr.getPartition(partition_id) != null) continue;

            // Build path — must be heap-allocated so it outlives this function
            var path_buf: [4096]u8 = undefined;
            const tmp_path = SpaceManager.partitionPath(d.path, partition_id, &path_buf) catch continue;

            // Dupe the path into allocator-owned memory
            const data_path = self.allocator.dupeZ(u8, tmp_path) catch continue;

            const dp = DataPartition.load(
                self.allocator,
                partition_id,
                0,
                data_path,
                &.{},
                d,
            ) catch {
                self.allocator.free(data_path);
                continue;
            };

            self.space_mgr.addPartition(dp);
            log.info("loaded partition {d} from {s}", .{ partition_id, d.path });
        }
    }

    // ─── Raft member operations ─────────────────────────────────

    const raft_iface = @import("raft.zig");
    const json_mod = @import("json.zig");

    fn handleRaftMemberAdd(self: *ServerInner, pkt: *const Packet) Packet {
        const dp = self.space_mgr.getPartition(pkt.partition_id) orelse {
            return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
        };

        // Parse peer info from request arg
        const peer = parsePeerFromArg(pkt.arg, self.allocator) orelse {
            return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
        };

        const rp = dp.raft_partition orelse {
            return Packet.errReply(pkt, proto.OP_ERR);
        };

        rp.changeMember(.add_node, peer) catch {
            return Packet.errReply(pkt, proto.OP_ERR);
        };

        // Also register the node with the raft server
        if (self.raft_server) |rs| {
            rs.addNode(peer.id, peer.address, peer.heartbeat_port, peer.replica_port);
        }

        return Packet.okReply(pkt);
    }

    fn handleRaftMemberRemove(self: *ServerInner, pkt: *const Packet) Packet {
        const dp = self.space_mgr.getPartition(pkt.partition_id) orelse {
            return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
        };

        const peer = parsePeerFromArg(pkt.arg, self.allocator) orelse {
            return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
        };

        const rp = dp.raft_partition orelse {
            return Packet.errReply(pkt, proto.OP_ERR);
        };

        rp.changeMember(.remove_node, peer) catch {
            return Packet.errReply(pkt, proto.OP_ERR);
        };

        return Packet.okReply(pkt);
    }

    fn handleTryToLeader(self: *ServerInner, pkt: *const Packet) Packet {
        const dp = self.space_mgr.getPartition(pkt.partition_id) orelse {
            return Packet.errReply(pkt, proto.OP_NOT_EXIST_ERR);
        };

        if (dp.raft_partition) |rp| {
            if (rp.isLeader()) {
                return Packet.okReply(pkt);
            }
        }

        // Not leader or no raft — return error
        return Packet.errReply(pkt, proto.OP_ERR);
    }

    fn parsePeerFromArg(arg: []const u8, allocator: Allocator) ?raft_iface.Peer {
        if (arg.len == 0) return null;

        const PeerReq = struct {
            id: ?u64 = null,
            addr: ?[]const u8 = null,
            heartbeat_port: ?u16 = null,
            replica_port: ?u16 = null,
        };

        const parsed = std.json.parseFromSlice(PeerReq, allocator, arg, .{
            .ignore_unknown_fields = true,
        }) catch return null;
        defer parsed.deinit();
        const pr = parsed.value;

        return raft_iface.Peer{
            .id = pr.id orelse return null,
            .address = pr.addr orelse "127.0.0.1",
            .heartbeat_port = pr.heartbeat_port orelse 17330,
            .replica_port = pr.replica_port orelse 17340,
        };
    }

    // ─── QoS handler ────────────────────────────────────────────

    const qos_mod = @import("qos.zig");

    fn handleQos(self: *ServerInner, pkt: *const Packet) Packet {
        if (pkt.arg.len == 0) {
            return Packet.okReply(pkt);
        }

        // Parse QoS config from JSON arg
        const JsonQos = struct {
            DiskReadFlow: u64 = 0,
            DiskWriteFlow: u64 = 0,
            DiskAsyncReadFlow: u64 = 0,
            DiskAsyncWriteFlow: u64 = 0,
            DiskReadIops: u64 = 0,
            DiskWriteIops: u64 = 0,
            DiskAsyncReadIops: u64 = 0,
            DiskAsyncWriteIops: u64 = 0,
            DiskDeleteIops: u64 = 0,
            DiskReadCc: u32 = 0,
            DiskWriteCc: u32 = 0,
            DiskAsyncReadCc: u32 = 0,
            DiskAsyncWriteCc: u32 = 0,
            DiskDeleteCc: u32 = 0,
        };

        const parsed = std.json.parseFromSlice(JsonQos, self.allocator, pkt.arg, .{
            .ignore_unknown_fields = true,
        }) catch {
            return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
        };
        defer parsed.deinit();
        const jq = parsed.value;

        const config = qos_mod.QosConfig{
            .disk_read_flow = jq.DiskReadFlow,
            .disk_write_flow = jq.DiskWriteFlow,
            .disk_async_read_flow = jq.DiskAsyncReadFlow,
            .disk_async_write_flow = jq.DiskAsyncWriteFlow,
            .disk_read_iops = jq.DiskReadIops,
            .disk_write_iops = jq.DiskWriteIops,
            .disk_async_read_iops = jq.DiskAsyncReadIops,
            .disk_async_write_iops = jq.DiskAsyncWriteIops,
            .disk_delete_iops = jq.DiskDeleteIops,
            .disk_read_cc = jq.DiskReadCc,
            .disk_write_cc = jq.DiskWriteCc,
            .disk_async_read_cc = jq.DiskAsyncReadCc,
            .disk_async_write_cc = jq.DiskAsyncWriteCc,
            .disk_delete_cc = jq.DiskDeleteCc,
        };

        // Apply to all disks
        const disk_count = self.space_mgr.diskCount();
        var i: usize = 0;
        while (i < disk_count) : (i += 1) {
            if (self.space_mgr.getDisk(i)) |d| {
                d.updateQosLimits(config);
            }
        }

        log.info("QoS limits updated: read_flow={d} write_flow={d} read_iops={d} write_iops={d}", .{
            config.disk_read_flow,
            config.disk_write_flow,
            config.disk_read_iops,
            config.disk_write_iops,
        });

        return Packet.okReply(pkt);
    }
};
