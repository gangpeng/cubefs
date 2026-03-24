// space_manager.zig — Manages all disks and data partitions.
// Mirrors Rust's cubefs-disk/src/space.rs.

const std = @import("std");
const Allocator = std.mem.Allocator;
const sharded_map_mod = @import("sharded_map.zig");
const disk_mod = @import("disk.zig");
const partition_mod = @import("partition.zig");
const Disk = disk_mod.Disk;
const DataPartition = partition_mod.DataPartition;

pub const SpaceManager = struct {
    /// Partitions keyed by partition_id.
    partitions: sharded_map_mod.ShardedMap(u64, *DataPartition),
    /// Disks keyed by hash of path.
    disks: std.ArrayList(*Disk),
    mu: std.Thread.Mutex,
    allocator: Allocator,

    pub fn init(allocator: Allocator) SpaceManager {
        return .{
            .partitions = sharded_map_mod.ShardedMap(u64, *DataPartition).init(allocator),
            .disks = std.ArrayList(*Disk).init(allocator),
            .mu = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SpaceManager) void {
        self.partitions.deinit();
        self.disks.deinit();
    }

    // ── Disk management ──────────────────────────────────────────

    pub fn addDisk(self: *SpaceManager, d: *Disk) !void {
        self.mu.lock();
        defer self.mu.unlock();
        try self.disks.append(d);
    }

    pub fn getDisk(self: *SpaceManager, idx: usize) ?*Disk {
        self.mu.lock();
        defer self.mu.unlock();
        if (idx >= self.disks.items.len) return null;
        return self.disks.items[idx];
    }

    pub fn diskCount(self: *SpaceManager) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return self.disks.items.len;
    }

    /// Select the disk with the most available space.
    pub fn selectDisk(self: *SpaceManager) ?*Disk {
        self.mu.lock();
        defer self.mu.unlock();
        var best: ?*Disk = null;
        var best_avail: u64 = 0;
        for (self.disks.items) |d| {
            if (!d.isGood()) continue;
            const avail = d.getAvailable();
            if (avail > best_avail) {
                best_avail = avail;
                best = d;
            }
        }
        return best;
    }

    // ── Partition management ─────────────────────────────────────

    pub fn addPartition(self: *SpaceManager, dp: *DataPartition) void {
        self.partitions.put(dp.partition_id, dp);
    }

    pub fn getPartition(self: *SpaceManager, partition_id: u64) ?*DataPartition {
        return self.partitions.get(partition_id);
    }

    pub fn removePartition(self: *SpaceManager, partition_id: u64) ?*DataPartition {
        return self.partitions.remove(partition_id);
    }

    pub fn partitionCount(self: *SpaceManager) usize {
        return self.partitions.count();
    }

    /// Collect all partition IDs.
    pub fn allPartitionIds(self: *SpaceManager, allocator: Allocator) ![]u64 {
        var list = std.ArrayList(u64).init(allocator);
        errdefer list.deinit();

        for (&self.partitions.shards) |*shard| {
            shard.lock.lockShared();
            defer shard.lock.unlockShared();
            var it = shard.map.keyIterator();
            while (it.next()) |key| {
                try list.append(key.*);
            }
        }
        return list.toOwnedSlice();
    }

    /// Collect all partitions.
    pub fn allPartitions(self: *SpaceManager, allocator: Allocator) ![]*DataPartition {
        var list = std.ArrayList(*DataPartition).init(allocator);
        errdefer list.deinit();

        for (&self.partitions.shards) |*shard| {
            shard.lock.lockShared();
            defer shard.lock.unlockShared();
            var it = shard.map.valueIterator();
            while (it.next()) |v| {
                try list.append(v.*);
            }
        }
        return list.toOwnedSlice();
    }

    /// Build the data path for a partition on a given disk.
    pub fn partitionPath(disk_path: []const u8, partition_id: u64, buf: []u8) ![:0]const u8 {
        return std.fmt.bufPrintZ(buf, "{s}/datapartition_{d}", .{ disk_path, partition_id });
    }
};
