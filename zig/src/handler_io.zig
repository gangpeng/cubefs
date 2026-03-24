// handler_io.zig — I/O opcode handlers: create, write, read, delete, watermarks.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const Packet = pkt_mod.Packet;
const types = @import("types.zig");
const constants = @import("constants.zig");
const crc32_mod = @import("crc32.zig");
const partition_mod = @import("partition.zig");
const json_mod = @import("json.zig");
const log = @import("log.zig");
const metrics_mod = @import("metrics.zig");
const DataPartition = partition_mod.DataPartition;

/// Handle OpCreateExtent
pub fn handleCreateExtent(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    _ = allocator;
    var extent_id = pkt.extent_id;

    // Auto-allocate ID if 0
    if (extent_id == 0) {
        extent_id = dp.store.nextExtentId();
    }

    dp.store.createExtent(extent_id) catch |e| {
        return errorPacket(pkt, e);
    };

    var reply = Packet.okReply(pkt);
    reply.extent_id = extent_id;
    return reply;
}

/// Handle OpWrite (append write)
pub fn handleWrite(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    return doWrite(pkt, dp, constants.WRITE_TYPE_APPEND, pkt.isSyncWrite(), allocator);
}

/// Handle OpRandomWrite
pub fn handleRandomWrite(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    return doWrite(pkt, dp, constants.WRITE_TYPE_RANDOM, pkt.isSyncWrite(), allocator);
}

fn doWrite(pkt: *const Packet, dp: *DataPartition, write_type: i32, is_sync: bool, allocator: Allocator) Packet {
    _ = allocator;
    if (pkt.data.len == 0) {
        return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
    }

    // Rate-limit writes
    dp.disk.acquireWrite();

    const start_ns = std.time.nanoTimestamp();

    const param = types.WriteParam{
        .extent_id = pkt.extent_id,
        .offset = pkt.extent_offset,
        .size = @intCast(pkt.size),
        .data = pkt.data,
        .crc = pkt.crc,
        .write_type = write_type,
        .is_sync = is_sync,
    };

    dp.store.writeExtent(&param) catch |e| {
        metrics_mod.global.recordError(false);
        return errorPacket(pkt, e);
    };

    const end_ns = std.time.nanoTimestamp();
    metrics_mod.write_latency.observeNs(start_ns, end_ns);
    metrics_mod.global.recordWrite(pkt.size);

    var reply = Packet.okReply(pkt);
    reply.crc = pkt.crc;
    reply.size = pkt.size;
    return reply;
}

/// Handle OpRead (read with CRC)
pub fn handleRead(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const read_size = pkt.size;
    if (read_size == 0) return Packet.okReply(pkt);

    // Rate-limit reads
    dp.disk.acquireRead();

    const buf = allocator.alloc(u8, read_size) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    const start_ns = std.time.nanoTimestamp();

    const offset: u64 = if (pkt.extent_offset >= 0) @intCast(pkt.extent_offset) else 0;
    const result = dp.store.readExtent(pkt.extent_id, offset, buf) catch |e| {
        allocator.free(buf);
        metrics_mod.global.recordError(true);
        return errorPacket(pkt, e);
    };

    const end_ns = std.time.nanoTimestamp();
    metrics_mod.read_latency.observeNs(start_ns, end_ns);
    metrics_mod.global.recordRead(result.bytes_read);

    var reply = Packet.okReply(pkt);
    reply.data = buf[0..result.bytes_read];
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = @intCast(result.bytes_read);
    reply.crc = result.crc;
    return reply;
}

/// Handle OpStreamRead / OpStreamFollowerRead (read without store-level CRC)
pub fn handleStreamRead(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const read_size = pkt.size;
    if (read_size == 0) return Packet.okReply(pkt);

    // Rate-limit reads
    dp.disk.acquireRead();

    const buf = allocator.alloc(u8, read_size) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    const offset: u64 = if (pkt.extent_offset >= 0) @intCast(pkt.extent_offset) else 0;
    const n = dp.store.readNoCrc(pkt.extent_id, offset, buf) catch |e| {
        allocator.free(buf);
        return errorPacket(pkt, e);
    };

    // Compute CRC on the read data
    const crc = crc32_mod.hash(buf[0..n]);

    var reply = Packet.okReply(pkt);
    reply.data = buf[0..n];
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = @intCast(n);
    reply.crc = crc;
    return reply;
}

/// Handle OpMarkDelete
pub fn handleMarkDelete(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    _ = allocator;

    // Rate-limit deletes
    dp.disk.acquireDelete();

    dp.store.markDelete(pkt.extent_id) catch |e| {
        return errorPacket(pkt, e);
    };
    metrics_mod.global.recordDelete();
    return Packet.okReply(pkt);
}

/// Handle OpGetAllWatermarks — JSON-encoded watermarks
pub fn handleGetWatermarks(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const watermarks = dp.store.getAllWatermarks(allocator) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    defer allocator.free(watermarks);

    // Build JSON array of watermark objects
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

/// Handle OpGetAppliedId — returns apply_id as big-endian u64
pub fn handleGetAppliedId(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const applied = dp.getAppliedId();
    const buf = allocator.alloc(u8, 8) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    std.mem.writeInt(u64, buf[0..8], applied, .big);

    var reply = Packet.okReply(pkt);
    reply.data = buf;
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = 8;
    return reply;
}

/// Handle OpGetPartitionSize — returns used_size (u64 BE) + extent_count (u64 BE)
pub fn handleGetPartitionSize(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const used = dp.usedSize();
    const count: u64 = @intCast(dp.extentCount());

    const buf = allocator.alloc(u8, 16) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    std.mem.writeInt(u64, buf[0..8], used, .big);
    std.mem.writeInt(u64, buf[8..16], count, .big);

    var reply = Packet.okReply(pkt);
    reply.data = buf;
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = 16;
    return reply;
}

/// Handle OpGetMaxExtentIdAndPartitionSize
pub fn handleGetMaxExtentIdAndPartitionSize(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const max_id = dp.store.base_extent_id.load(.acquire);
    const used = dp.usedSize();
    const count: u64 = @intCast(dp.extentCount());

    const buf = allocator.alloc(u8, 24) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    std.mem.writeInt(u64, buf[0..8], max_id, .big);
    std.mem.writeInt(u64, buf[8..16], used, .big);
    std.mem.writeInt(u64, buf[16..24], count, .big);

    var reply = Packet.okReply(pkt);
    reply.data = buf;
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = 24;
    return reply;
}

/// Handle OpBroadcastMinAppliedId
pub fn handleBroadcastMinAppliedId(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    _ = allocator;
    // Parse the min applied_id from arg (8 bytes big-endian)
    if (pkt.arg.len >= 8) {
        const min_id = std.mem.readInt(u64, pkt.arg[0..8], .big);
        const current = dp.getAppliedId();
        if (min_id > current) {
            dp.setAppliedId(min_id);
        }
    }
    return Packet.okReply(pkt);
}

/// Handle OpReadTinyDeleteRecord — return binary tiny delete records.
pub fn handleReadTinyDeleteRecord(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const records = dp.store.readTinyDeleteRecords(allocator) catch {
        return Packet.okReply(pkt);
    };
    defer if (records.len > 0) allocator.free(records);

    if (records.len == 0) {
        return Packet.okReply(pkt);
    }

    // Encode as binary: each record is 24 bytes (3 x u64 big-endian)
    const record_size: usize = 24;
    const total_size = records.len * record_size;
    const buf = allocator.alloc(u8, total_size) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    for (records, 0..) |rec, i| {
        const base = i * record_size;
        std.mem.writeInt(u64, buf[base..][0..8], rec.extent_id, .big);
        std.mem.writeInt(u64, buf[base + 8 ..][0..8], rec.offset, .big);
        std.mem.writeInt(u64, buf[base + 16 ..][0..8], rec.size, .big);
    }

    var reply = Packet.okReply(pkt);
    reply.data = buf;
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = @intCast(total_size);
    return reply;
}

// ─── Repair Read Handlers ──────────────────────────────────────────

/// Handle OpExtentRepairRead — read extent data for repair streaming.
/// Reads from the specified offset/size and returns the data with CRC.
pub fn handleExtentRepairRead(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    const read_size = pkt.size;
    if (read_size == 0) return Packet.okReply(pkt);

    const buf = allocator.alloc(u8, read_size) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    const offset: u64 = if (pkt.extent_offset >= 0) @intCast(pkt.extent_offset) else 0;
    const n = dp.store.readNoCrc(pkt.extent_id, offset, buf) catch |e| {
        allocator.free(buf);
        return errorPacket(pkt, e);
    };

    const crc = crc32_mod.hash(buf[0..n]);

    var reply = Packet.okReply(pkt);
    reply.data = buf[0..n];
    reply.data_owned = true;
    reply.allocator = allocator;
    reply.size = @intCast(n);
    reply.crc = crc;
    return reply;
}

/// Handle OpTinyExtentRepairRead — read tiny extent data for repair.
/// Same as regular repair read but operates on tiny extents.
pub fn handleTinyExtentRepairRead(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    return handleExtentRepairRead(pkt, dp, allocator);
}

/// Handle OpSnapshotExtentRepairRead — read snapshot extent data for repair.
pub fn handleSnapshotExtentRepairRead(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    return handleExtentRepairRead(pkt, dp, allocator);
}

// ─── Backup Handlers ──────────────────────────────────────────────

/// Handle OpBackupRead — read extent data during backup/GC.
/// Identical to a stream read: reads data and computes CRC on the fly.
pub fn handleBackupRead(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    return handleStreamRead(pkt, dp, allocator);
}

/// Handle OpBackupWrite — write extent data during backup/GC restore.
/// Identical to a normal write but always synchronous.
pub fn handleBackupWrite(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    return doWrite(pkt, dp, constants.WRITE_TYPE_RANDOM, true, allocator);
}

// ─── Batch Lock/Unlock Handlers ───────────────────────────────────

/// Handle OpBatchLockNormalExtent — lock a set of extents for GC/backup.
/// Arg contains JSON array of extent lock entries: [{ExtentId, GcFlag}, ...]
pub fn handleBatchLockExtent(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    if (pkt.arg.len == 0) {
        return Packet.okReply(pkt);
    }

    const extent_store_mod = @import("extent_store.zig");
    const GcFlag = extent_store_mod.GcFlag;
    const ExtentLock = extent_store_mod.ExtentLock;

    // Parse JSON array of lock entries from arg
    const JsonLockEntry = struct {
        ExtentId: u64 = 0,
        GcFlag: u8 = 0,
    };

    const parsed = std.json.parseFromSlice([]const JsonLockEntry, allocator, pkt.arg, .{
        .ignore_unknown_fields = true,
    }) catch {
        return Packet.errReply(pkt, proto.OP_ARG_MISMATCH_ERR);
    };
    defer parsed.deinit();

    // Convert to internal ExtentLock format
    var locks = allocator.alloc(ExtentLock, parsed.value.len) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };
    defer allocator.free(locks);

    for (parsed.value, 0..) |entry, i| {
        locks[i] = .{
            .extent_id = entry.ExtentId,
            .flag = switch (entry.GcFlag) {
                1 => GcFlag.gc_mark,
                2 => GcFlag.gc_delete,
                else => GcFlag.normal,
            },
        };
    }

    dp.store.batchLockExtents(locks) catch {
        return Packet.errReply(pkt, proto.OP_ERR);
    };

    return Packet.okReply(pkt);
}

/// Handle OpBatchUnlockNormalExtent — unlock all GC/backup locks on a partition.
pub fn handleBatchUnlockExtent(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    _ = allocator;
    dp.store.batchUnlockExtents();
    return Packet.okReply(pkt);
}

// ─── Decommission Handler ─────────────────────────────────────────

/// Handle OpDecommissionDataPartition — mark partition as read-only for decommission.
pub fn handleDecommission(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    _ = allocator;
    dp.setStatus(partition_mod.PARTITION_STATUS_READ_ONLY);
    dp.persistApplyId();
    dp.persistMeta();
    log.info("partition {d}: decommissioned (set read-only)", .{dp.partition_id});
    return Packet.okReply(pkt);
}

// ─── Notify Replicas to Repair ────────────────────────────────────

/// Handle OpNotifyReplicasToRepair — trigger a repair check.
/// This is a hint from the master; the actual repair runs asynchronously.
pub fn handleNotifyReplicasToRepair(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    _ = allocator;
    log.info("partition {d}: received repair notification", .{dp.partition_id});
    return Packet.okReply(pkt);
}

// ─── Stop Repair ──────────────────────────────────────────────────

/// Handle OpStopDataPartitionRepair — stop repair for a partition.
pub fn handleStopRepair(pkt: *const Packet, dp: *DataPartition, allocator: Allocator) Packet {
    _ = allocator;
    log.info("partition {d}: repair stopped by master", .{dp.partition_id});
    return Packet.okReply(pkt);
}

/// Map storage error to error reply packet.
fn errorPacket(pkt: *const Packet, e: anytype) Packet {
    const code: u8 = switch (e) {
        error.ExtentNotFound => proto.OP_NOT_EXIST_ERR,
        error.ExtentExists => proto.OP_EXIST_ERR,
        error.ExtentDeleted => proto.OP_NOT_EXIST_ERR,
        error.NoSpace => proto.OP_DISK_NO_SPACE_ERR,
        error.CrcMismatch => proto.OP_ARG_MISMATCH_ERR,
        error.ForbidWrite => proto.OP_FORBID_ERR,
        error.ExtentFull => proto.OP_NO_SPACE_ERR,
        error.BrokenDisk => proto.OP_DISK_ERR,
        error.ExtentLocked => proto.OP_AGAIN,
        error.NotLeader => proto.OP_TRY_OTHER_ADDR,
        error.RaftSubmitFailed => proto.OP_ERR,
        else => proto.OP_ERR,
    };
    return Packet.errReply(pkt, code);
}
