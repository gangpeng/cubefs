// protocol.zig — CubeFS wire protocol opcodes, flags, result codes, helpers.
// Matches Go's proto/packet.go exactly for wire-compatibility.

const std = @import("std");

// ─── Magic & Header Sizes ─────────────────────────────────────────
pub const PROTO_MAGIC: u8 = 0xFF;
pub const PACKET_HEADER_SIZE: usize = 57;
pub const PACKET_HEADER_VER_SIZE: usize = 65;
pub const PACKET_HEADER_PROTO_VER_SIZE: usize = 69;

// ─── Extent Type Flags ────────────────────────────────────────────
pub const TINY_EXTENT_TYPE: u8 = 0x00;
pub const NORMAL_EXTENT_TYPE: u8 = 0x01;
pub const MULTI_VERSION_FLAG: u8 = 0x80;
pub const VERSION_LIST_FLAG: u8 = 0x40;
pub const PACKET_PROTOCOL_VERSION_FLAG: u8 = 0x10;

// ─── Protocol Versions ───────────────────────────────────────────
pub const PACKET_PROTO_VERSION_0: u32 = 0;
pub const PACKET_PROTO_VERSION_1: u32 = 1;

// ─── Special constants ───────────────────────────────────────────
pub const SPECIAL_REPLICA_CNT: u8 = 127;
pub const FOLLOWER_READ_FLAG: u8 = 'F';
pub const ADDR_SPLIT: []const u8 = "/";

// ─── Deadlines (seconds) ─────────────────────────────────────────
pub const WRITE_DEADLINE_TIME: u64 = 5;
pub const READ_DEADLINE_TIME: u64 = 5;
pub const BATCH_DELETE_EXTENT_READ_DEADLINE_TIME: u64 = 120;
pub const GET_ALL_WATERMARKS_DEADLINE_TIME: u64 = 60;

// ─── Data Partition Creation Types ───────────────────────────────
pub const NORMAL_CREATE_DATA_PARTITION: u8 = 0;
pub const DECOMMISSIONED_CREATE_DATA_PARTITION: u8 = 1;

// ─── Client → DataNode Opcodes ───────────────────────────────────
pub const OP_INIT_RESULT_CODE: u8 = 0x00;
pub const OP_CREATE_EXTENT: u8 = 0x01;
pub const OP_MARK_DELETE: u8 = 0x02;
pub const OP_WRITE: u8 = 0x03;
pub const OP_READ: u8 = 0x04;
pub const OP_STREAM_READ: u8 = 0x05;
pub const OP_STREAM_FOLLOWER_READ: u8 = 0x06;
pub const OP_GET_ALL_WATERMARKS: u8 = 0x07;
pub const OP_NOTIFY_REPLICAS_TO_REPAIR: u8 = 0x08;
pub const OP_EXTENT_REPAIR_READ: u8 = 0x09;
pub const OP_BROADCAST_MIN_APPLIED_ID: u8 = 0x0A;
pub const OP_RANDOM_WRITE: u8 = 0x0F;
pub const OP_GET_APPLIED_ID: u8 = 0x10;
pub const OP_GET_PARTITION_SIZE: u8 = 0x11;
pub const OP_SYNC_RANDOM_WRITE: u8 = 0x12;
pub const OP_SYNC_WRITE: u8 = 0x13;
pub const OP_READ_TINY_DELETE_RECORD: u8 = 0x14;
pub const OP_TINY_EXTENT_REPAIR_READ: u8 = 0x15;
pub const OP_GET_MAX_EXTENT_ID_AND_PARTITION_SIZE: u8 = 0x16;
pub const OP_SNAPSHOT_EXTENT_REPAIR_READ: u8 = 0x17;
pub const OP_SNAPSHOT_EXTENT_REPAIR_RSP: u8 = 0x18;

// ─── Multi-version / Snapshot Opcodes ────────────────────────────
pub const OP_RANDOM_WRITE_APPEND: u8 = 0xB1;
pub const OP_SYNC_RANDOM_WRITE_APPEND: u8 = 0xB2;
pub const OP_RANDOM_WRITE_VER: u8 = 0xB3;
pub const OP_SYNC_RANDOM_WRITE_VER: u8 = 0xB4;
pub const OP_SYNC_RANDOM_WRITE_VER_RSP: u8 = 0xB5;
pub const OP_TRY_WRITE_APPEND: u8 = 0xB6;
pub const OP_SYNC_TRY_WRITE_APPEND: u8 = 0xB7;
pub const OP_VERSION_OP: u8 = 0xB8;

// ─── Master → DataNode Opcodes ───────────────────────────────────
pub const OP_CREATE_DATA_PARTITION: u8 = 0x60;
pub const OP_DELETE_DATA_PARTITION: u8 = 0x61;
pub const OP_LOAD_DATA_PARTITION: u8 = 0x62;
pub const OP_DATA_NODE_HEARTBEAT: u8 = 0x63;
pub const OP_REPLICATE_FILE: u8 = 0x64;
pub const OP_DELETE_FILE: u8 = 0x65;
pub const OP_DECOMMISSION_DATA_PARTITION: u8 = 0x66;
pub const OP_ADD_DATA_PARTITION_RAFT_MEMBER: u8 = 0x67;
pub const OP_REMOVE_DATA_PARTITION_RAFT_MEMBER: u8 = 0x68;
pub const OP_DATA_PARTITION_TRY_TO_LEADER: u8 = 0x69;
pub const OP_QOS: u8 = 0x6A;
pub const OP_STOP_DATA_PARTITION_REPAIR: u8 = 0x6B;
pub const OP_RECOVER_DATA_REPLICA_META: u8 = 0x6C;
pub const OP_RECOVER_BACKUP_DATA_REPLICA: u8 = 0x6D;
pub const OP_RECOVER_BAD_DISK: u8 = 0x6E;
pub const OP_QUERY_BAD_DISK_RECOVER_PROGRESS: u8 = 0x6F;
pub const OP_DELETE_BACKUP_DIRECTORIES: u8 = 0x80;
pub const OP_DELETE_LOST_DISK: u8 = 0x8A;
pub const OP_RELOAD_DISK: u8 = 0x8B;
pub const OP_SET_REPAIRING_STATUS: u8 = 0x8C;

// ─── Backup / Lock Opcodes ───────────────────────────────────────
pub const OP_BATCH_LOCK_NORMAL_EXTENT: u8 = 0x57;
pub const OP_BATCH_UNLOCK_NORMAL_EXTENT: u8 = 0x58;
pub const OP_BACKUP_READ: u8 = 0x59;
pub const OP_BACKUP_WRITE: u8 = 0x5A;

// ─── Version Operations ──────────────────────────────────────────
pub const OP_VERSION_OPERATION: u8 = 0xD5;
pub const OP_SPLIT_MARK_DELETE: u8 = 0xD6;
pub const OP_TRY_OTHER_EXTENT: u8 = 0xD7;

// ─── Result Codes ────────────────────────────────────────────────
pub const OP_OK: u8 = 0xF0;
pub const OP_ERR: u8 = 0xF8;
pub const OP_DISK_ERR: u8 = 0xF7;
pub const OP_DISK_NO_SPACE_ERR: u8 = 0xF6;
pub const OP_EXIST_ERR: u8 = 0xFA;
pub const OP_NOT_EXIST_ERR: u8 = 0xF5;
pub const OP_TRY_OTHER_ADDR: u8 = 0xFC;
pub const OP_INTRA_GROUP_NET_ERR: u8 = 0xF3;
pub const OP_ARG_MISMATCH_ERR: u8 = 0xF4;
pub const OP_AGAIN: u8 = 0xF9;
pub const OP_NO_SPACE_ERR: u8 = 0xEE;
pub const OP_FORBID_ERR: u8 = 0xEF;
pub const OP_DIR_QUOTA: u8 = 0xF1;
pub const OP_CONFLICT_EXTENTS_ERR: u8 = 0xF2;
pub const OP_INODE_FULL_ERR: u8 = 0xFB;
pub const OP_NOT_PERM: u8 = 0xFD;
pub const OP_NOT_EMPTY: u8 = 0xFE;
pub const OP_PING: u8 = 0xFF;

// ─── Helper Functions ────────────────────────────────────────────

/// Returns true when this packet must be forwarded to downstream followers.
pub fn isForwardPacket(remaining_followers: u8) bool {
    return remaining_followers > 0 and remaining_followers != SPECIAL_REPLICA_CNT;
}

/// Returns true for any opcode that represents a read operation.
/// Read requests carry 0 data bytes on the wire (size field is desired read length).
pub fn isReadOperation(opcode: u8) bool {
    return switch (opcode) {
        OP_READ,
        OP_STREAM_READ,
        OP_STREAM_FOLLOWER_READ,
        OP_EXTENT_REPAIR_READ,
        OP_TINY_EXTENT_REPAIR_READ,
        OP_SNAPSHOT_EXTENT_REPAIR_READ,
        OP_BACKUP_READ,
        => true,
        else => false,
    };
}

/// Returns true for random-write opcodes.
pub fn isRandomWrite(opcode: u8) bool {
    return switch (opcode) {
        OP_RANDOM_WRITE,
        OP_SYNC_RANDOM_WRITE,
        OP_RANDOM_WRITE_APPEND,
        OP_SYNC_RANDOM_WRITE_APPEND,
        OP_RANDOM_WRITE_VER,
        OP_SYNC_RANDOM_WRITE_VER,
        OP_TRY_WRITE_APPEND,
        OP_SYNC_TRY_WRITE_APPEND,
        => true,
        else => false,
    };
}

/// Returns true for sync-write opcodes (require fdatasync).
pub fn isSyncWrite(opcode: u8) bool {
    return switch (opcode) {
        OP_SYNC_WRITE,
        OP_SYNC_RANDOM_WRITE,
        OP_SYNC_RANDOM_WRITE_APPEND,
        OP_SYNC_RANDOM_WRITE_VER,
        OP_SYNC_TRY_WRITE_APPEND,
        => true,
        else => false,
    };
}

/// Returns true for any write opcode.
pub fn isWriteOperation(opcode: u8) bool {
    return switch (opcode) {
        OP_WRITE,
        OP_SYNC_WRITE,
        OP_RANDOM_WRITE,
        OP_SYNC_RANDOM_WRITE,
        OP_RANDOM_WRITE_APPEND,
        OP_SYNC_RANDOM_WRITE_APPEND,
        OP_RANDOM_WRITE_VER,
        OP_SYNC_RANDOM_WRITE_VER,
        OP_TRY_WRITE_APPEND,
        OP_SYNC_TRY_WRITE_APPEND,
        OP_BACKUP_WRITE,
        => true,
        else => false,
    };
}

/// Human-readable opcode name (for logging).
pub fn opcodeName(op: u8) []const u8 {
    return switch (op) {
        OP_INIT_RESULT_CODE => "InitResultCode",
        OP_CREATE_EXTENT => "CreateExtent",
        OP_MARK_DELETE => "MarkDelete",
        OP_WRITE => "Write",
        OP_READ => "Read",
        OP_STREAM_READ => "StreamRead",
        OP_STREAM_FOLLOWER_READ => "StreamFollowerRead",
        OP_GET_ALL_WATERMARKS => "GetAllWatermarks",
        OP_NOTIFY_REPLICAS_TO_REPAIR => "NotifyReplicasToRepair",
        OP_EXTENT_REPAIR_READ => "ExtentRepairRead",
        OP_BROADCAST_MIN_APPLIED_ID => "BroadcastMinAppliedId",
        OP_RANDOM_WRITE => "RandomWrite",
        OP_GET_APPLIED_ID => "GetAppliedId",
        OP_GET_PARTITION_SIZE => "GetPartitionSize",
        OP_SYNC_RANDOM_WRITE => "SyncRandomWrite",
        OP_SYNC_WRITE => "SyncWrite",
        OP_READ_TINY_DELETE_RECORD => "ReadTinyDeleteRecord",
        OP_TINY_EXTENT_REPAIR_READ => "TinyExtentRepairRead",
        OP_GET_MAX_EXTENT_ID_AND_PARTITION_SIZE => "GetMaxExtentIdAndPartitionSize",
        OP_SNAPSHOT_EXTENT_REPAIR_READ => "SnapshotExtentRepairRead",
        OP_SNAPSHOT_EXTENT_REPAIR_RSP => "SnapshotExtentRepairRsp",
        OP_RANDOM_WRITE_APPEND => "RandomWriteAppend",
        OP_SYNC_RANDOM_WRITE_APPEND => "SyncRandomWriteAppend",
        OP_RANDOM_WRITE_VER => "RandomWriteVer",
        OP_SYNC_RANDOM_WRITE_VER => "SyncRandomWriteVer",
        OP_SYNC_RANDOM_WRITE_VER_RSP => "SyncRandomWriteVerRsp",
        OP_TRY_WRITE_APPEND => "TryWriteAppend",
        OP_SYNC_TRY_WRITE_APPEND => "SyncTryWriteAppend",
        OP_VERSION_OP => "VersionOp",
        OP_CREATE_DATA_PARTITION => "CreateDataPartition",
        OP_DELETE_DATA_PARTITION => "DeleteDataPartition",
        OP_LOAD_DATA_PARTITION => "LoadDataPartition",
        OP_DATA_NODE_HEARTBEAT => "DataNodeHeartbeat",
        OP_REPLICATE_FILE => "ReplicateFile",
        OP_DELETE_FILE => "DeleteFile",
        OP_DECOMMISSION_DATA_PARTITION => "DecommissionDataPartition",
        OP_ADD_DATA_PARTITION_RAFT_MEMBER => "AddDataPartitionRaftMember",
        OP_REMOVE_DATA_PARTITION_RAFT_MEMBER => "RemoveDataPartitionRaftMember",
        OP_DATA_PARTITION_TRY_TO_LEADER => "DataPartitionTryToLeader",
        OP_QOS => "QoS",
        OP_BATCH_LOCK_NORMAL_EXTENT => "BatchLockNormalExtent",
        OP_BATCH_UNLOCK_NORMAL_EXTENT => "BatchUnlockNormalExtent",
        OP_BACKUP_READ => "BackupRead",
        OP_BACKUP_WRITE => "BackupWrite",
        OP_OK => "Ok",
        OP_ERR => "Err",
        OP_DISK_ERR => "DiskErr",
        OP_DISK_NO_SPACE_ERR => "DiskNoSpaceErr",
        OP_EXIST_ERR => "ExistErr",
        OP_NOT_EXIST_ERR => "NotExistErr",
        OP_AGAIN => "Again",
        OP_PING => "Ping",
        else => "Unknown",
    };
}

/// Parse follower addresses from the arg field. Addresses are `/`-separated.
/// e.g. "/10.0.0.1:17310/10.0.0.2:17310" → ["10.0.0.1:17310", "10.0.0.2:17310"]
pub fn parseFollowerAddrs(arg: []const u8) std.ArrayList([]const u8) {
    // Caller is expected to use a simpler split. See packet.zig for the helper.
    _ = arg;
    @compileError("use splitFollowerAddrs in packet.zig instead");
}

/// Returns the extra header length based on extent_type flags.
pub fn extraHeaderLen(extent_type: u8) usize {
    if (extent_type & PACKET_PROTOCOL_VERSION_FLAG != 0) {
        return 12; // 8 (ver_seq) + 4 (proto_version)
    }
    if (extent_type & MULTI_VERSION_FLAG != 0) {
        return 8; // 8 (ver_seq)
    }
    return 0;
}

/// Returns total header size based on extent_type flags.
pub fn headerSize(extent_type: u8) usize {
    return PACKET_HEADER_SIZE + extraHeaderLen(extent_type);
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "isForwardPacket returns true for remaining followers > 0" {
    try testing.expect(isForwardPacket(1));
    try testing.expect(isForwardPacket(2));
    try testing.expect(isForwardPacket(100));
}

test "isForwardPacket returns false for 0 or SPECIAL_REPLICA_CNT" {
    try testing.expect(!isForwardPacket(0));
    try testing.expect(!isForwardPacket(SPECIAL_REPLICA_CNT));
}

test "isReadOperation classifies read opcodes correctly" {
    try testing.expect(isReadOperation(OP_READ));
    try testing.expect(isReadOperation(OP_STREAM_READ));
    try testing.expect(isReadOperation(OP_STREAM_FOLLOWER_READ));
    try testing.expect(isReadOperation(OP_EXTENT_REPAIR_READ));
    try testing.expect(isReadOperation(OP_TINY_EXTENT_REPAIR_READ));
    try testing.expect(isReadOperation(OP_SNAPSHOT_EXTENT_REPAIR_READ));
    try testing.expect(isReadOperation(OP_BACKUP_READ));
    // Non-reads
    try testing.expect(!isReadOperation(OP_WRITE));
    try testing.expect(!isReadOperation(OP_RANDOM_WRITE));
    try testing.expect(!isReadOperation(OP_CREATE_EXTENT));
    try testing.expect(!isReadOperation(OP_MARK_DELETE));
}

test "isRandomWrite classifies random write opcodes correctly" {
    try testing.expect(isRandomWrite(OP_RANDOM_WRITE));
    try testing.expect(isRandomWrite(OP_SYNC_RANDOM_WRITE));
    try testing.expect(isRandomWrite(OP_RANDOM_WRITE_APPEND));
    try testing.expect(isRandomWrite(OP_SYNC_RANDOM_WRITE_APPEND));
    try testing.expect(isRandomWrite(OP_RANDOM_WRITE_VER));
    try testing.expect(isRandomWrite(OP_SYNC_RANDOM_WRITE_VER));
    try testing.expect(isRandomWrite(OP_TRY_WRITE_APPEND));
    try testing.expect(isRandomWrite(OP_SYNC_TRY_WRITE_APPEND));
    // Not random writes
    try testing.expect(!isRandomWrite(OP_WRITE));
    try testing.expect(!isRandomWrite(OP_SYNC_WRITE));
    try testing.expect(!isRandomWrite(OP_READ));
}

test "isSyncWrite classifies sync write opcodes correctly" {
    try testing.expect(isSyncWrite(OP_SYNC_WRITE));
    try testing.expect(isSyncWrite(OP_SYNC_RANDOM_WRITE));
    try testing.expect(isSyncWrite(OP_SYNC_RANDOM_WRITE_APPEND));
    try testing.expect(isSyncWrite(OP_SYNC_RANDOM_WRITE_VER));
    try testing.expect(isSyncWrite(OP_SYNC_TRY_WRITE_APPEND));
    // Not sync writes
    try testing.expect(!isSyncWrite(OP_WRITE));
    try testing.expect(!isSyncWrite(OP_RANDOM_WRITE));
    try testing.expect(!isSyncWrite(OP_TRY_WRITE_APPEND));
    try testing.expect(!isSyncWrite(OP_READ));
}

test "isWriteOperation classifies all write opcodes correctly" {
    try testing.expect(isWriteOperation(OP_WRITE));
    try testing.expect(isWriteOperation(OP_SYNC_WRITE));
    try testing.expect(isWriteOperation(OP_RANDOM_WRITE));
    try testing.expect(isWriteOperation(OP_SYNC_RANDOM_WRITE));
    try testing.expect(isWriteOperation(OP_RANDOM_WRITE_APPEND));
    try testing.expect(isWriteOperation(OP_SYNC_RANDOM_WRITE_APPEND));
    try testing.expect(isWriteOperation(OP_RANDOM_WRITE_VER));
    try testing.expect(isWriteOperation(OP_SYNC_RANDOM_WRITE_VER));
    try testing.expect(isWriteOperation(OP_TRY_WRITE_APPEND));
    try testing.expect(isWriteOperation(OP_SYNC_TRY_WRITE_APPEND));
    try testing.expect(isWriteOperation(OP_BACKUP_WRITE));
    // Not writes
    try testing.expect(!isWriteOperation(OP_READ));
    try testing.expect(!isWriteOperation(OP_CREATE_EXTENT));
    try testing.expect(!isWriteOperation(OP_MARK_DELETE));
    try testing.expect(!isWriteOperation(OP_GET_ALL_WATERMARKS));
}

test "opcodeName returns correct names for known opcodes" {
    try testing.expectEqualStrings("Write", opcodeName(OP_WRITE));
    try testing.expectEqualStrings("Read", opcodeName(OP_READ));
    try testing.expectEqualStrings("CreateExtent", opcodeName(OP_CREATE_EXTENT));
    try testing.expectEqualStrings("MarkDelete", opcodeName(OP_MARK_DELETE));
    try testing.expectEqualStrings("SyncWrite", opcodeName(OP_SYNC_WRITE));
    try testing.expectEqualStrings("RandomWrite", opcodeName(OP_RANDOM_WRITE));
    try testing.expectEqualStrings("StreamRead", opcodeName(OP_STREAM_READ));
    try testing.expectEqualStrings("DataNodeHeartbeat", opcodeName(OP_DATA_NODE_HEARTBEAT));
    try testing.expectEqualStrings("CreateDataPartition", opcodeName(OP_CREATE_DATA_PARTITION));
    try testing.expectEqualStrings("Ok", opcodeName(OP_OK));
    try testing.expectEqualStrings("Err", opcodeName(OP_ERR));
    try testing.expectEqualStrings("Ping", opcodeName(OP_PING));
}

test "opcodeName returns Unknown for unrecognized opcodes" {
    try testing.expectEqualStrings("Unknown", opcodeName(0xAA));
    try testing.expectEqualStrings("Unknown", opcodeName(0xBB));
}

test "opcodeName returns InitResultCode for OP_INIT_RESULT_CODE" {
    try testing.expectEqualStrings("InitResultCode", opcodeName(OP_INIT_RESULT_CODE));
}

test "extraHeaderLen returns correct sizes" {
    // No flags
    try testing.expectEqual(@as(usize, 0), extraHeaderLen(NORMAL_EXTENT_TYPE));
    try testing.expectEqual(@as(usize, 0), extraHeaderLen(TINY_EXTENT_TYPE));

    // MULTI_VERSION_FLAG only
    try testing.expectEqual(@as(usize, 8), extraHeaderLen(MULTI_VERSION_FLAG));
    try testing.expectEqual(@as(usize, 8), extraHeaderLen(NORMAL_EXTENT_TYPE | MULTI_VERSION_FLAG));

    // PACKET_PROTOCOL_VERSION_FLAG (takes precedence since checked first)
    try testing.expectEqual(@as(usize, 12), extraHeaderLen(PACKET_PROTOCOL_VERSION_FLAG));
    try testing.expectEqual(@as(usize, 12), extraHeaderLen(PACKET_PROTOCOL_VERSION_FLAG | MULTI_VERSION_FLAG));
}

test "headerSize includes base header plus extra" {
    try testing.expectEqual(PACKET_HEADER_SIZE, headerSize(0));
    try testing.expectEqual(PACKET_HEADER_SIZE + 8, headerSize(MULTI_VERSION_FLAG));
    try testing.expectEqual(PACKET_HEADER_SIZE + 12, headerSize(PACKET_PROTOCOL_VERSION_FLAG));
}

test "read and write operation sets are disjoint" {
    // Every read operation should not be a write operation
    const read_ops = [_]u8{
        OP_READ,               OP_STREAM_READ,           OP_STREAM_FOLLOWER_READ,
        OP_EXTENT_REPAIR_READ, OP_TINY_EXTENT_REPAIR_READ, OP_SNAPSHOT_EXTENT_REPAIR_READ,
        OP_BACKUP_READ,
    };
    for (read_ops) |op| {
        try testing.expect(!isWriteOperation(op));
    }
}

test "sync writes are a subset of all writes" {
    const sync_ops = [_]u8{
        OP_SYNC_WRITE, OP_SYNC_RANDOM_WRITE, OP_SYNC_RANDOM_WRITE_APPEND,
        OP_SYNC_RANDOM_WRITE_VER, OP_SYNC_TRY_WRITE_APPEND,
    };
    for (sync_ops) |op| {
        try testing.expect(isWriteOperation(op));
    }
}

test "random writes are a subset of all writes" {
    const rw_ops = [_]u8{
        OP_RANDOM_WRITE,       OP_SYNC_RANDOM_WRITE,       OP_RANDOM_WRITE_APPEND,
        OP_SYNC_RANDOM_WRITE_APPEND, OP_RANDOM_WRITE_VER, OP_SYNC_RANDOM_WRITE_VER,
        OP_TRY_WRITE_APPEND,   OP_SYNC_TRY_WRITE_APPEND,
    };
    for (rw_ops) |op| {
        try testing.expect(isWriteOperation(op));
    }
}
