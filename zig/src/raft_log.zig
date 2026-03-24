// raft_log.zig — Binary serialization for Raft log entries.
// Wire format matches Go's binary.BigEndian encoding for cross-language compat.
//
// Format:
// +--------+--------+----------+--------+------+-----+------+
// | Magic  | Opcode | ExtentID | Offset | Size | CRC | Data |
// | 4 bytes| 1 byte | 8 bytes  | 8 bytes|8 bytes|4 bytes| N  |
// +--------+--------+----------+--------+------+-----+------+
// Total header: 33 bytes

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Magic number identifying a binary write command in the Raft log.
pub const RAFT_CMD_MAGIC: u32 = 0x000000FF;

/// Fixed header size (before variable-length data).
pub const RAFT_LOG_HEADER_SIZE: usize = 33;

/// A parsed Raft log command.
pub const RaftCommand = struct {
    magic: u32,
    opcode: u8,
    extent_id: u64,
    offset: i64,
    size: i64,
    crc: u32,
    data: []const u8,
};

/// Marshal a Raft command into a byte buffer.
/// Caller owns the returned slice.
pub fn marshalRaftLog(
    allocator: Allocator,
    opcode: u8,
    extent_id: u64,
    offset: i64,
    size: i64,
    data: []const u8,
    crc: u32,
) ![]u8 {
    const total_len = RAFT_LOG_HEADER_SIZE + data.len;
    const buf = try allocator.alloc(u8, total_len);

    // Magic (4 bytes, big-endian)
    std.mem.writeInt(u32, buf[0..4], RAFT_CMD_MAGIC, .big);
    // Opcode (1 byte)
    buf[4] = opcode;
    // ExtentID (8 bytes, big-endian)
    std.mem.writeInt(u64, buf[5..13], extent_id, .big);
    // Offset (8 bytes, big-endian)
    std.mem.writeInt(i64, buf[13..21], offset, .big);
    // Size (8 bytes, big-endian)
    std.mem.writeInt(i64, buf[21..29], size, .big);
    // CRC (4 bytes, big-endian)
    std.mem.writeInt(u32, buf[29..33], crc, .big);
    // Data
    if (data.len > 0) {
        @memcpy(buf[33..], data);
    }

    return buf;
}

/// Unmarshal a Raft command from a byte buffer.
/// The returned `data` slice is a view into the input buffer (no copy).
pub fn unmarshalRaftLog(raw: []const u8) !RaftCommand {
    if (raw.len < RAFT_LOG_HEADER_SIZE) {
        return error.InvalidData;
    }

    const magic = std.mem.readInt(u32, raw[0..4], .big);
    if (magic != RAFT_CMD_MAGIC) {
        return error.InvalidData;
    }

    const opcode = raw[4];
    const extent_id = std.mem.readInt(u64, raw[5..13], .big);
    const offset = std.mem.readInt(i64, raw[13..21], .big);
    const size = std.mem.readInt(i64, raw[21..29], .big);
    const crc = std.mem.readInt(u32, raw[29..33], .big);
    const data = raw[33..];

    return RaftCommand{
        .magic = magic,
        .opcode = opcode,
        .extent_id = extent_id,
        .offset = offset,
        .size = size,
        .crc = crc,
        .data = data,
    };
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "marshal/unmarshal round-trip" {
    const alloc = testing.allocator;
    const data = "hello world";
    const buf = try marshalRaftLog(alloc, 0x0F, 42, 4096, @intCast(data.len), data, 0xDEADBEEF);
    defer alloc.free(buf);

    const cmd = try unmarshalRaftLog(buf);
    try testing.expectEqual(RAFT_CMD_MAGIC, cmd.magic);
    try testing.expectEqual(@as(u8, 0x0F), cmd.opcode);
    try testing.expectEqual(@as(u64, 42), cmd.extent_id);
    try testing.expectEqual(@as(i64, 4096), cmd.offset);
    try testing.expectEqual(@as(i64, @intCast(data.len)), cmd.size);
    try testing.expectEqual(@as(u32, 0xDEADBEEF), cmd.crc);
    try testing.expectEqualStrings("hello world", cmd.data);
}

test "marshal/unmarshal empty data" {
    const alloc = testing.allocator;
    const buf = try marshalRaftLog(alloc, 0x02, 1, 0, 0, &.{}, 0);
    defer alloc.free(buf);

    try testing.expectEqual(RAFT_LOG_HEADER_SIZE, buf.len);

    const cmd = try unmarshalRaftLog(buf);
    try testing.expectEqual(@as(u64, 1), cmd.extent_id);
    try testing.expectEqual(@as(usize, 0), cmd.data.len);
}

test "unmarshal rejects short buffer" {
    const short = [_]u8{ 0, 0, 0 };
    try testing.expectError(error.InvalidData, unmarshalRaftLog(&short));
}

test "unmarshal rejects bad magic" {
    var buf: [RAFT_LOG_HEADER_SIZE]u8 = undefined;
    @memset(&buf, 0);
    // Set wrong magic
    std.mem.writeInt(u32, buf[0..4], 0xBADBAD, .big);
    try testing.expectError(error.InvalidData, unmarshalRaftLog(&buf));
}

test "big-endian encoding matches Go wire format" {
    const alloc = testing.allocator;
    const buf = try marshalRaftLog(alloc, 0x03, 0x0102030405060708, 0x1112131415161718, 256, &.{}, 0xAABBCCDD);
    defer alloc.free(buf);

    // Verify magic bytes
    try testing.expectEqual(@as(u8, 0x00), buf[0]);
    try testing.expectEqual(@as(u8, 0x00), buf[1]);
    try testing.expectEqual(@as(u8, 0x00), buf[2]);
    try testing.expectEqual(@as(u8, 0xFF), buf[3]);

    // Verify opcode
    try testing.expectEqual(@as(u8, 0x03), buf[4]);

    // Verify extent_id big-endian: 0x01 02 03 04 05 06 07 08
    try testing.expectEqual(@as(u8, 0x01), buf[5]);
    try testing.expectEqual(@as(u8, 0x08), buf[12]);
}
