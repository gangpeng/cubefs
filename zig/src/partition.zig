// partition.zig — DataPartition: owns an ExtentStore plus metadata.
// Mirrors Go's datanode/DataPartition with Raft integration.

const std = @import("std");
const Allocator = std.mem.Allocator;
const err_mod = @import("error.zig");
const extent_store_mod = @import("extent_store.zig");
const disk_mod = @import("disk.zig");
const raft_mod = @import("raft.zig");
const raft_log_mod = @import("raft_log.zig");
const raft_server_mod = @import("raft/server.zig");
const partition_meta = @import("partition_meta.zig");
const types = @import("types.zig");
const log = @import("log.zig");
const ExtentStore = extent_store_mod.ExtentStore;
const Disk = disk_mod.Disk;
const RaftPartition = raft_mod.RaftPartition;
const StateMachine = raft_mod.StateMachine;
const WriteParam = types.WriteParam;
const RaftServer = raft_server_mod.RaftServer;

pub const PARTITION_STATUS_RECOVERING: i32 = 0;
pub const PARTITION_STATUS_READ_ONLY: i32 = 1;
pub const PARTITION_STATUS_READ_WRITE: i32 = 2;
pub const PARTITION_STATUS_UNAVAILABLE: i32 = -1;

/// Interval between apply ID persistence (in apply count).
const APPLY_ID_PERSIST_INTERVAL: u64 = 1000;

pub const DataPartition = struct {
    partition_id: u64,
    volume_id: u64,
    partition_size: std.atomic.Value(u64),
    store: *ExtentStore,
    replicas: []const []const u8,
    applied_id: std.atomic.Value(u64),
    status: std.atomic.Value(i32),
    disk: *Disk,
    data_path: [:0]const u8,
    allocator: Allocator,

    // Raft fields
    raft_partition: ?RaftPartition,
    is_raft_leader: std.atomic.Value(bool),
    peers: std.ArrayList(raft_mod.Peer),
    last_truncate_id: std.atomic.Value(u64),
    apply_count: std.atomic.Value(u64),

    /// Create a new DataPartition (creates the ExtentStore directory).
    pub fn init(
        allocator: Allocator,
        partition_id: u64,
        volume_id: u64,
        data_path: [:0]const u8,
        replicas: []const []const u8,
        disk: *Disk,
    ) !*DataPartition {
        const store = ExtentStore.create(allocator, data_path, partition_id) catch {
            return error.IoError;
        };

        const self = try allocator.create(DataPartition);
        self.* = .{
            .partition_id = partition_id,
            .volume_id = volume_id,
            .partition_size = std.atomic.Value(u64).init(0),
            .store = store,
            .replicas = replicas,
            .applied_id = std.atomic.Value(u64).init(store.applyId()),
            .status = std.atomic.Value(i32).init(PARTITION_STATUS_READ_WRITE),
            .disk = disk,
            .data_path = data_path,
            .allocator = allocator,
            .raft_partition = null,
            .is_raft_leader = std.atomic.Value(bool).init(false),
            .peers = std.ArrayList(raft_mod.Peer).init(allocator),
            .last_truncate_id = std.atomic.Value(u64).init(0),
            .apply_count = std.atomic.Value(u64).init(0),
        };

        // Persist initial metadata
        self.persistMeta();

        return self;
    }

    /// Load an existing DataPartition from disk.
    pub fn load(
        allocator: Allocator,
        partition_id: u64,
        volume_id: u64,
        data_path: [:0]const u8,
        replicas: []const []const u8,
        disk: *Disk,
    ) !*DataPartition {
        return init(allocator, partition_id, volume_id, data_path, replicas, disk);
    }

    pub fn deinit(self: *DataPartition) void {
        self.persistMeta();
        self.stopRaft();
        self.store.persistApplyId() catch {};
        self.store.close();
        self.store.destroy();
        self.peers.deinit();
        self.allocator.destroy(self);
    }

    pub fn getStatus(self: *const DataPartition) i32 {
        return self.status.load(.acquire);
    }

    pub fn setStatus(self: *DataPartition, status: i32) void {
        self.status.store(status, .release);
    }

    pub fn isNormal(self: *const DataPartition) bool {
        return self.getStatus() == PARTITION_STATUS_READ_WRITE;
    }

    pub fn getSize(self: *const DataPartition) u64 {
        return self.partition_size.load(.acquire);
    }

    pub fn setSize(self: *DataPartition, size: u64) void {
        self.partition_size.store(size, .release);
    }

    pub fn getAppliedId(self: *const DataPartition) u64 {
        return self.applied_id.load(.acquire);
    }

    pub fn setAppliedId(self: *DataPartition, id: u64) void {
        self.applied_id.store(id, .release);
        self.store.setApplyId(id);
    }

    pub fn usedSize(self: *DataPartition) u64 {
        return self.store.usedSize();
    }

    pub fn extentCount(self: *DataPartition) usize {
        return self.store.extentCount();
    }

    /// Whether this partition's Raft instance is the leader.
    pub fn isRaftLeader(self: *const DataPartition) bool {
        if (self.raft_partition) |rp| {
            return rp.isLeader();
        }
        return self.is_raft_leader.load(.acquire);
    }

    /// Set the Raft partition implementation.
    pub fn setRaftPartition(self: *DataPartition, rp: RaftPartition) void {
        self.raft_partition = rp;
    }

    /// Submit a random write through Raft consensus.
    /// The write is serialized into a Raft log entry and submitted.
    /// The actual write happens when the log entry is committed (via applyRaftCommand).
    pub fn randomWriteSubmit(self: *DataPartition, param: *const WriteParam) err_mod.Error!void {
        const rp = self.raft_partition orelse {
            // No raft — direct write fallback
            return self.store.writeExtent(param);
        };

        if (!rp.isLeader()) {
            return error.NotLeader;
        }

        const cmd = raft_log_mod.marshalRaftLog(
            self.allocator,
            @as(u8, @intCast(param.write_type)),
            param.extent_id,
            param.offset,
            param.size,
            param.data,
            param.crc,
        ) catch return error.RaftSubmitFailed;
        defer self.allocator.free(cmd);

        rp.submit(cmd) catch return error.RaftSubmitFailed;
    }

    /// Apply a committed Raft log entry (FSM callback).
    pub fn applyRaftCommand(self: *DataPartition, command: []const u8, index: u64) !void {
        const cmd = raft_log_mod.unmarshalRaftLog(command) catch {
            log.warn("partition {d}: failed to unmarshal raft command at index {d}", .{ self.partition_id, index });
            return;
        };

        const param = WriteParam{
            .extent_id = cmd.extent_id,
            .offset = cmd.offset,
            .size = cmd.size,
            .data = cmd.data,
            .crc = cmd.crc,
            .write_type = @intCast(cmd.opcode),
            .is_sync = false,
        };

        self.store.writeExtent(&param) catch |e| {
            log.warn("partition {d}: apply write failed for extent {d}: {}", .{ self.partition_id, cmd.extent_id, e });
            return e;
        };

        self.setAppliedId(index);

        // Periodically persist apply ID
        const count = self.apply_count.fetchAdd(1, .seq_cst);
        if (count % APPLY_ID_PERSIST_INTERVAL == 0) {
            self.store.persistApplyId() catch {};
        }
    }

    /// Apply a committed membership change (FSM callback).
    pub fn applyMemberChange(self: *DataPartition, ct: raft_mod.ConfChangeType, peer: raft_mod.Peer, index: u64) !void {
        switch (ct) {
            .add_node => {
                self.peers.append(peer) catch {};
                log.info("partition {d}: added raft peer {d} at index {d}", .{ self.partition_id, peer.id, index });
            },
            .remove_node => {
                var i: usize = 0;
                while (i < self.peers.items.len) {
                    if (self.peers.items[i].id == peer.id) {
                        _ = self.peers.orderedRemove(i);
                        break;
                    }
                    i += 1;
                }
                log.info("partition {d}: removed raft peer {d} at index {d}", .{ self.partition_id, peer.id, index });
            },
        }
        self.setAppliedId(index);
        self.persistMeta();
    }

    /// Get the state machine interface for this partition (for Raft integration).
    pub fn stateMachine(self: *DataPartition) StateMachine {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &sm_vtable,
        };
    }

    /// Start Raft for this partition (stub: sets leader flag).
    pub fn startRaft(self: *DataPartition) void {
        self.is_raft_leader.store(true, .release);
    }

    /// Start Raft for this partition using a RaftServer instance.
    pub fn startRaftWithServer(self: *DataPartition, rs: *RaftServer) void {
        const sm = self.stateMachine();
        const instance = rs.createPartition(
            self.partition_id,
            self.peers.items,
            sm,
        ) catch |e| {
            log.err("partition {d}: failed to create raft instance: {}", .{ self.partition_id, e });
            // Fall back to stub behavior
            self.startRaft();
            return;
        };

        const rp = instance.partition();
        self.setRaftPartition(rp);
        log.info("partition {d}: raft started", .{self.partition_id});
    }

    /// Stop Raft for this partition.
    pub fn stopRaft(self: *DataPartition) void {
        if (self.raft_partition) |rp| {
            rp.stop();
            self.raft_partition = null;
        }
        self.is_raft_leader.store(false, .release);
    }

    /// Persist the apply ID to disk.
    pub fn persistApplyId(self: *DataPartition) void {
        self.store.persistApplyId() catch {};
    }

    /// Build and persist partition metadata to the META file.
    pub fn persistMeta(self: *DataPartition) void {
        const peers = self.peers.items;
        var peer_infos_buf: [128]partition_meta.PeerInfo = undefined;
        const peer_count = @min(peers.len, peer_infos_buf.len);
        for (peers[0..peer_count], 0..) |p, i| {
            peer_infos_buf[i] = .{
                .id = p.id,
                .addr = p.address,
                .heartbeat_port = p.heartbeat_port,
                .replica_port = p.replica_port,
            };
        }

        const meta = partition_meta.PartitionMetadata{
            .volume_id = self.volume_id,
            .partition_id = self.partition_id,
            .partition_size = self.getSize(),
            .peers = peer_infos_buf[0..peer_count],
            .replica_num = @intCast(peer_count),
            .applied_id = self.getAppliedId(),
            .last_truncate_id = self.last_truncate_id.load(.acquire),
            .is_repairing = false,
        };

        partition_meta.persistMetadata(self.data_path, &meta, self.allocator) catch |e| {
            log.warn("partition {d}: failed to persist metadata: {}", .{ self.partition_id, e });
        };
    }

    // ─── StateMachine vtable ──────────────────────────────────────

    fn smApply(ctx: *anyopaque, cmd: []const u8, index: u64) anyerror!void {
        const self: *DataPartition = @ptrCast(@alignCast(ctx));
        try self.applyRaftCommand(cmd, index);
    }

    fn smApplyMemberChange(ctx: *anyopaque, ct: raft_mod.ConfChangeType, peer: raft_mod.Peer, index: u64) anyerror!void {
        const self: *DataPartition = @ptrCast(@alignCast(ctx));
        try self.applyMemberChange(ct, peer, index);
    }

    fn smSnapshot(ctx: *anyopaque) u64 {
        const self: *DataPartition = @ptrCast(@alignCast(ctx));
        return self.getAppliedId();
    }

    fn smHandleLeaderChange(ctx: *anyopaque, leader_id: u64) void {
        const self: *DataPartition = @ptrCast(@alignCast(ctx));
        _ = leader_id;
        if (self.raft_partition) |rp| {
            self.is_raft_leader.store(rp.isLeader(), .release);
        }
    }

    fn smHandleFatal(ctx: *anyopaque) void {
        const self: *DataPartition = @ptrCast(@alignCast(ctx));
        self.setStatus(PARTITION_STATUS_UNAVAILABLE);
        log.err("partition {d}: fatal raft error", .{self.partition_id});
    }

    const sm_vtable = StateMachine.VTable{
        .apply = smApply,
        .applyMemberChange = smApplyMemberChange,
        .snapshot = smSnapshot,
        .handleLeaderChange = smHandleLeaderChange,
        .handleFatal = smHandleFatal,
    };
};
