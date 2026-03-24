// packet.zig — CubeFS wire packet: marshal / unmarshal / helpers.
// Wire-compatible with Go's proto.Packet (big-endian, 57-byte base header).

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("protocol.zig");

/// A CubeFS wire packet. Owns `arg` and `data` slices (caller must free via deinit).
pub const Packet = struct {
    magic: u8 = proto.PROTO_MAGIC,
    extent_type: u8 = 0,
    opcode: u8 = 0,
    result_code: u8 = proto.OP_INIT_RESULT_CODE,
    remaining_followers: u8 = 0,
    crc: u32 = 0,
    size: u32 = 0,
    arg_len: u32 = 0,
    partition_id: u64 = 0,
    extent_id: u64 = 0,
    extent_offset: i64 = 0,
    req_id: i64 = 0,
    kernel_offset: u64 = 0,
    // Extended fields
    ver_seq: u64 = 0,
    proto_version: u32 = 0,
    // Variable-length payloads (owned slices)
    arg: []const u8 = &.{},
    data: []const u8 = &.{},

    allocator: ?Allocator = null,
    arg_owned: bool = false,
    data_owned: bool = false,

    pub fn deinit(self: *Packet) void {
        if (self.allocator) |alloc| {
            if (self.arg_owned and self.arg.len > 0) alloc.free(@constCast(self.arg));
            if (self.data_owned and self.data.len > 0) alloc.free(@constCast(self.data));
        }
        self.arg = &.{};
        self.data = &.{};
        self.arg_owned = false;
        self.data_owned = false;
    }

    // ── Marshal ──────────────────────────────────────────────────

    /// Write the base 57-byte header to `buf`. Caller must ensure buf.len >= 57.
    pub fn marshalHeader(self: *const Packet, buf: []u8) void {
        std.debug.assert(buf.len >= proto.PACKET_HEADER_SIZE);
        buf[0] = self.magic;
        buf[1] = self.extent_type;
        buf[2] = self.opcode;
        buf[3] = self.result_code;
        buf[4] = self.remaining_followers;
        std.mem.writeInt(u32, buf[5..9], self.crc, .big);
        std.mem.writeInt(u32, buf[9..13], self.size, .big);
        std.mem.writeInt(u32, buf[13..17], self.arg_len, .big);
        std.mem.writeInt(u64, buf[17..25], self.partition_id, .big);
        std.mem.writeInt(u64, buf[25..33], self.extent_id, .big);
        std.mem.writeInt(i64, buf[33..41], self.extent_offset, .big);
        std.mem.writeInt(i64, buf[41..49], self.req_id, .big);
        std.mem.writeInt(u64, buf[49..57], self.kernel_offset, .big);
    }

    /// Write extended header fields (ver_seq, proto_version) after the base 57 bytes.
    pub fn marshalExtraFields(self: *const Packet, buf: []u8) void {
        const extra = proto.extraHeaderLen(self.extent_type);
        if (extra >= 8) {
            std.mem.writeInt(u64, buf[0..8], self.ver_seq, .big);
        }
        if (extra >= 12) {
            std.mem.writeInt(u32, buf[8..12], self.proto_version, .big);
        }
    }

    /// Serialize the complete packet (header + extra + arg + data) into an allocated buffer.
    pub fn marshal(self: *const Packet, allocator: Allocator) ![]u8 {
        const hdr_size = proto.headerSize(self.extent_type);
        const total = hdr_size + self.arg.len + self.data.len;
        const buf = try allocator.alloc(u8, total);
        self.marshalHeader(buf[0..proto.PACKET_HEADER_SIZE]);
        if (hdr_size > proto.PACKET_HEADER_SIZE) {
            self.marshalExtraFields(buf[proto.PACKET_HEADER_SIZE..hdr_size]);
        }
        if (self.arg.len > 0) {
            @memcpy(buf[hdr_size .. hdr_size + self.arg.len], self.arg);
        }
        if (self.data.len > 0) {
            @memcpy(buf[hdr_size + self.arg.len ..], self.data);
        }
        return buf;
    }

    // ── Unmarshal ────────────────────────────────────────────────

    /// Parse the base 57-byte header. Returns a Packet with zero-length arg/data.
    pub fn unmarshalHeader(buf: []const u8) error{InvalidMagic}!Packet {
        if (buf.len < proto.PACKET_HEADER_SIZE) return error.InvalidMagic;
        if (buf[0] != proto.PROTO_MAGIC) return error.InvalidMagic;
        return Packet{
            .magic = buf[0],
            .extent_type = buf[1],
            .opcode = buf[2],
            .result_code = buf[3],
            .remaining_followers = buf[4],
            .crc = std.mem.readInt(u32, buf[5..9], .big),
            .size = std.mem.readInt(u32, buf[9..13], .big),
            .arg_len = std.mem.readInt(u32, buf[13..17], .big),
            .partition_id = std.mem.readInt(u64, buf[17..25], .big),
            .extent_id = std.mem.readInt(u64, buf[25..33], .big),
            .extent_offset = std.mem.readInt(i64, buf[33..41], .big),
            .req_id = std.mem.readInt(i64, buf[41..49], .big),
            .kernel_offset = std.mem.readInt(u64, buf[49..57], .big),
        };
    }

    /// Parse extra header fields from bytes following the base header.
    pub fn readExtraFields(self: *Packet, extra: []const u8) void {
        const extra_len = proto.extraHeaderLen(self.extent_type);
        if (extra_len >= 8 and extra.len >= 8) {
            self.ver_seq = std.mem.readInt(u64, extra[0..8], .big);
        }
        if (extra_len >= 12 and extra.len >= 12) {
            self.proto_version = std.mem.readInt(u32, extra[8..12], .big);
        }
    }

    // ── Convenience constructors ─────────────────────────────────

    /// Create an OK reply matching the given request.
    pub fn okReply(req: *const Packet) Packet {
        return Packet{
            .magic = proto.PROTO_MAGIC,
            .extent_type = req.extent_type,
            .opcode = req.opcode,
            .result_code = proto.OP_OK,
            .remaining_followers = req.remaining_followers,
            .crc = 0,
            .size = 0,
            .arg_len = 0,
            .partition_id = req.partition_id,
            .extent_id = req.extent_id,
            .extent_offset = req.extent_offset,
            .req_id = req.req_id,
            .kernel_offset = req.kernel_offset,
            .ver_seq = req.ver_seq,
            .proto_version = req.proto_version,
        };
    }

    /// Create an error reply matching the given request.
    pub fn errReply(req: *const Packet, result_code: u8) Packet {
        var reply = okReply(req);
        reply.result_code = result_code;
        return reply;
    }

    /// Create an error reply with a message in the data field.
    pub fn errReplyWithMsg(req: *const Packet, result_code: u8, msg: []const u8, allocator: Allocator) !Packet {
        var reply = errReply(req, result_code);
        if (msg.len > 0) {
            const data = try allocator.dupe(u8, msg);
            reply.data = data;
            reply.data_owned = true;
            reply.allocator = allocator;
            reply.size = @intCast(data.len);
        }
        return reply;
    }

    /// Returns true if the packet should be forwarded to followers.
    pub fn isForward(self: *const Packet) bool {
        return proto.isForwardPacket(self.remaining_followers);
    }

    /// Returns true if this is a read request (carries 0 data bytes on wire).
    pub fn isReadRequest(self: *const Packet) bool {
        return proto.isReadOperation(self.opcode) and self.result_code == proto.OP_INIT_RESULT_CODE;
    }

    /// Returns true for random-write opcodes.
    pub fn isRandomWrite(self: *const Packet) bool {
        return proto.isRandomWrite(self.opcode);
    }

    /// Returns true for sync-write opcodes.
    pub fn isSyncWrite(self: *const Packet) bool {
        return proto.isSyncWrite(self.opcode);
    }

    /// Total header size for this packet (57, 65, or 69 bytes).
    pub fn headerSize(self: *const Packet) usize {
        return proto.headerSize(self.extent_type);
    }
};

/// Split follower addresses from slash-separated arg bytes.
/// e.g. "/10.0.0.1:17310/10.0.0.2:17310" → ["10.0.0.1:17310", "10.0.0.2:17310"]
/// Caller owns the returned slice and must free it.
pub fn splitFollowerAddrs(arg: []const u8, allocator: Allocator) ![][]const u8 {
    if (arg.len == 0) return &.{};

    var list = std.ArrayList([]const u8).init(allocator);
    errdefer list.deinit();

    var iter = std.mem.splitScalar(u8, arg, '/');
    while (iter.next()) |segment| {
        if (segment.len > 0) {
            try list.append(segment);
        }
    }
    return list.toOwnedSlice();
}

/// Encode follower addresses back into slash-separated format.
/// e.g. ["10.0.0.1:17310", "10.0.0.2:17310"] → "/10.0.0.1:17310/10.0.0.2:17310"
pub fn encodeFollowerAddrs(addrs: []const []const u8, allocator: Allocator) ![]u8 {
    if (addrs.len == 0) return try allocator.alloc(u8, 0);

    var total_len: usize = 0;
    for (addrs) |addr| {
        total_len += 1 + addr.len; // "/" + addr
    }

    const buf = try allocator.alloc(u8, total_len);
    var pos: usize = 0;
    for (addrs) |addr| {
        buf[pos] = '/';
        pos += 1;
        @memcpy(buf[pos .. pos + addr.len], addr);
        pos += addr.len;
    }
    return buf;
}

// ── Tests ────────────────────────────────────────────────────────

test "marshal and unmarshal base header roundtrip" {
    var pkt = Packet{
        .opcode = proto.OP_WRITE,
        .result_code = proto.OP_INIT_RESULT_CODE,
        .remaining_followers = 2,
        .crc = 0xDEADBEEF,
        .size = 4096,
        .arg_len = 32,
        .partition_id = 12345,
        .extent_id = 67890,
        .extent_offset = -1024,
        .req_id = 9999,
        .kernel_offset = 0,
    };

    var buf: [proto.PACKET_HEADER_SIZE]u8 = undefined;
    pkt.marshalHeader(&buf);

    const decoded = try Packet.unmarshalHeader(&buf);
    try std.testing.expectEqual(proto.PROTO_MAGIC, decoded.magic);
    try std.testing.expectEqual(proto.OP_WRITE, decoded.opcode);
    try std.testing.expectEqual(@as(u8, 2), decoded.remaining_followers);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), decoded.crc);
    try std.testing.expectEqual(@as(u32, 4096), decoded.size);
    try std.testing.expectEqual(@as(u32, 32), decoded.arg_len);
    try std.testing.expectEqual(@as(u64, 12345), decoded.partition_id);
    try std.testing.expectEqual(@as(u64, 67890), decoded.extent_id);
    try std.testing.expectEqual(@as(i64, -1024), decoded.extent_offset);
    try std.testing.expectEqual(@as(i64, 9999), decoded.req_id);
}

test "marshal and unmarshal with proto version" {
    var pkt = Packet{
        .opcode = proto.OP_READ,
        .extent_type = proto.PACKET_PROTOCOL_VERSION_FLAG,
        .ver_seq = 42,
        .proto_version = proto.PACKET_PROTO_VERSION_1,
        .partition_id = 100,
        .extent_id = 200,
    };

    var buf: [proto.PACKET_HEADER_PROTO_VER_SIZE]u8 = undefined;
    pkt.marshalHeader(buf[0..proto.PACKET_HEADER_SIZE]);
    pkt.marshalExtraFields(buf[proto.PACKET_HEADER_SIZE..proto.PACKET_HEADER_PROTO_VER_SIZE]);

    var decoded = try Packet.unmarshalHeader(buf[0..proto.PACKET_HEADER_SIZE]);
    decoded.readExtraFields(buf[proto.PACKET_HEADER_SIZE..proto.PACKET_HEADER_PROTO_VER_SIZE]);

    try std.testing.expectEqual(@as(u64, 42), decoded.ver_seq);
    try std.testing.expectEqual(proto.PACKET_PROTO_VERSION_1, decoded.proto_version);
    try std.testing.expectEqual(@as(u64, 100), decoded.partition_id);
    try std.testing.expectEqual(@as(u64, 200), decoded.extent_id);
}

test "ok and error replies" {
    const req = Packet{
        .opcode = proto.OP_WRITE,
        .partition_id = 1,
        .extent_id = 2,
        .req_id = 42,
    };

    const ok = Packet.okReply(&req);
    try std.testing.expectEqual(proto.OP_OK, ok.result_code);
    try std.testing.expectEqual(proto.OP_WRITE, ok.opcode);
    try std.testing.expectEqual(@as(i64, 42), ok.req_id);

    const err_pkt = Packet.errReply(&req, proto.OP_NOT_EXIST_ERR);
    try std.testing.expectEqual(proto.OP_NOT_EXIST_ERR, err_pkt.result_code);
}

test "split and encode follower addrs" {
    const alloc = std.testing.allocator;

    const addrs = try splitFollowerAddrs("/10.0.0.1:17310/10.0.0.2:17310", alloc);
    defer alloc.free(addrs);
    try std.testing.expectEqual(@as(usize, 2), addrs.len);
    try std.testing.expectEqualStrings("10.0.0.1:17310", addrs[0]);
    try std.testing.expectEqualStrings("10.0.0.2:17310", addrs[1]);

    const encoded = try encodeFollowerAddrs(addrs, alloc);
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings("/10.0.0.1:17310/10.0.0.2:17310", encoded);
}

test "split empty arg" {
    const alloc = std.testing.allocator;
    const addrs = try splitFollowerAddrs("", alloc);
    try std.testing.expectEqual(@as(usize, 0), addrs.len);
}

test "isReadRequest" {
    const read_req = Packet{ .opcode = proto.OP_READ, .result_code = proto.OP_INIT_RESULT_CODE };
    try std.testing.expect(read_req.isReadRequest());

    const read_resp = Packet{ .opcode = proto.OP_READ, .result_code = proto.OP_OK };
    try std.testing.expect(!read_resp.isReadRequest());

    const write_req = Packet{ .opcode = proto.OP_WRITE, .result_code = proto.OP_INIT_RESULT_CODE };
    try std.testing.expect(!write_req.isReadRequest());
}
