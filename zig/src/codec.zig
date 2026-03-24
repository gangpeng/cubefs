// codec.zig — TCP framing for CubeFS packets.
// Reads/writes complete packets from/to a TCP stream.
// Implements the same state machine as Rust's PacketCodec.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("protocol.zig");
const pkt_mod = @import("packet.zig");
const Packet = pkt_mod.Packet;

pub const CodecError = error{
    InvalidMagic,
    PayloadTooLarge,
    ConnectionClosed,
    Unexpected,
    OutOfMemory,
    InputOutput,
};

/// Maximum single-packet payload (data + arg) = 64 MB. Guard against corruption.
pub const MAX_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// Read exactly `buf.len` bytes from a stream. Returns error on EOF or I/O error.
fn readExact(reader: anytype, buf: []u8) !void {
    var total_read: usize = 0;
    while (total_read < buf.len) {
        const n = reader.read(buf[total_read..]) catch |err| {
            return switch (err) {
                error.ConnectionResetByPeer, error.BrokenPipe => error.ConnectionClosed,
                else => error.InputOutput,
            };
        };
        if (n == 0) return error.ConnectionClosed;
        total_read += n;
    }
}

/// Read one complete packet from the stream.
/// The returned Packet owns its `arg` and `data` slices (allocated via `allocator`).
pub fn readPacket(reader: anytype, allocator: Allocator) !Packet {
    // 1. Read base 57-byte header
    var hdr_buf: [proto.PACKET_HEADER_SIZE]u8 = undefined;
    try readExact(reader, &hdr_buf);

    var packet = Packet.unmarshalHeader(&hdr_buf) catch return error.InvalidMagic;
    packet.allocator = allocator;

    // 2. Read extra header fields if present
    const extra_len = proto.extraHeaderLen(packet.extent_type);
    if (extra_len > 0) {
        var extra_buf: [12]u8 = undefined;
        try readExact(reader, extra_buf[0..extra_len]);
        packet.readExtraFields(extra_buf[0..extra_len]);
    }

    // 3. Determine wire data size
    // Read requests carry 0 data bytes on wire (size field = desired read length).
    // Write responses carry 0 data bytes on wire (size field echoes the request).
    const is_response = (packet.result_code != proto.OP_INIT_RESULT_CODE);
    const wire_data_size: u32 = if (packet.isReadRequest())
        0
    else if (is_response and proto.isWriteOperation(packet.opcode))
        0
    else
        packet.size;

    // 4. Validate payload size
    const total_body: u64 = @as(u64, packet.arg_len) + @as(u64, wire_data_size);
    if (total_body > MAX_PAYLOAD_SIZE) return error.PayloadTooLarge;

    // 5. Read arg
    if (packet.arg_len > 0) {
        const arg_buf = try allocator.alloc(u8, packet.arg_len);
        errdefer allocator.free(arg_buf);
        try readExact(reader, arg_buf);
        packet.arg = arg_buf;
        packet.arg_owned = true;
    }

    // 6. Read data
    if (wire_data_size > 0) {
        const data_buf = try allocator.alloc(u8, wire_data_size);
        errdefer allocator.free(data_buf);
        try readExact(reader, data_buf);
        packet.data = data_buf;
        packet.data_owned = true;
    }

    return packet;
}

/// Write one complete packet to the stream.
pub fn writePacket(writer: anytype, packet: *const Packet) !void {
    // 1. Write base header
    var hdr_buf: [proto.PACKET_HEADER_SIZE]u8 = undefined;
    packet.marshalHeader(&hdr_buf);
    try writeAll(writer, &hdr_buf);

    // 2. Write extra fields
    const extra_len = proto.extraHeaderLen(packet.extent_type);
    if (extra_len > 0) {
        var extra_buf: [12]u8 = undefined;
        packet.marshalExtraFields(extra_buf[0..extra_len]);
        try writeAll(writer, extra_buf[0..extra_len]);
    }

    // 3. Write arg
    if (packet.arg.len > 0) {
        try writeAll(writer, packet.arg);
    }

    // 4. Write data
    if (packet.data.len > 0) {
        try writeAll(writer, packet.data);
    }
}

fn writeAll(writer: anytype, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const n = writer.write(data[written..]) catch return error.InputOutput;
        if (n == 0) return error.ConnectionClosed;
        written += n;
    }
}

/// Write a packet and flush to ensure it is sent immediately.
pub fn writePacketFlush(writer: anytype, packet: *const Packet) !void {
    try writePacket(writer, packet);
    // If the writer supports flush (e.g. BufferedWriter), flush it.
    if (@hasDecl(@TypeOf(writer), "flush")) {
        try writer.flush();
    }
}

// ── Tests ────────────────────────────────────────────────────────

const FixedBufferStream = std.io.FixedBufferStream([]u8);

test "codec: write then read base packet roundtrip" {
    const allocator = std.testing.allocator;
    var buf: [4096]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);

    // Write
    var pkt = Packet{
        .opcode = proto.OP_WRITE,
        .remaining_followers = 1,
        .crc = 0x12345678,
        .size = 5,
        .arg_len = 3,
        .partition_id = 100,
        .extent_id = 200,
        .extent_offset = 4096,
        .req_id = 42,
        .arg = "abc",
        .data = "hello",
    };
    try writePacket(fbs.writer(), &pkt);

    // Read back
    fbs.pos = 0;
    var decoded = try readPacket(fbs.reader(), allocator);
    defer decoded.deinit();

    try std.testing.expectEqual(proto.OP_WRITE, decoded.opcode);
    try std.testing.expectEqual(@as(u32, 0x12345678), decoded.crc);
    try std.testing.expectEqual(@as(u32, 5), decoded.size);
    try std.testing.expectEqual(@as(u64, 100), decoded.partition_id);
    try std.testing.expectEqual(@as(u64, 200), decoded.extent_id);
    try std.testing.expectEqual(@as(i64, 4096), decoded.extent_offset);
    try std.testing.expectEqualStrings("abc", decoded.arg);
    try std.testing.expectEqualStrings("hello", decoded.data);
}

test "codec: read request carries zero data on wire" {
    const allocator = std.testing.allocator;
    var buf: [4096]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);

    // A read request: size = 8192 (desired read size), but wire data = 0
    var pkt = Packet{
        .opcode = proto.OP_READ,
        .result_code = proto.OP_INIT_RESULT_CODE,
        .size = 8192,
        .partition_id = 1,
        .extent_id = 2,
    };
    // Write only header (no data bytes on wire for read request)
    try writePacket(fbs.writer(), &pkt);

    fbs.pos = 0;
    var decoded = try readPacket(fbs.reader(), allocator);
    defer decoded.deinit();

    try std.testing.expectEqual(proto.OP_READ, decoded.opcode);
    try std.testing.expectEqual(@as(u32, 8192), decoded.size); // preserved
    try std.testing.expectEqual(@as(usize, 0), decoded.data.len); // no wire data
}

test "codec: packet with proto version extension" {
    const allocator = std.testing.allocator;
    var buf: [4096]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);

    var pkt = Packet{
        .opcode = proto.OP_WRITE,
        .extent_type = proto.PACKET_PROTOCOL_VERSION_FLAG | proto.NORMAL_EXTENT_TYPE,
        .ver_seq = 999,
        .proto_version = proto.PACKET_PROTO_VERSION_1,
        .size = 4,
        .data = "test",
    };
    try writePacket(fbs.writer(), &pkt);

    fbs.pos = 0;
    var decoded = try readPacket(fbs.reader(), allocator);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u64, 999), decoded.ver_seq);
    try std.testing.expectEqual(proto.PACKET_PROTO_VERSION_1, decoded.proto_version);
    try std.testing.expectEqualStrings("test", decoded.data);
}

test "codec: multiple packets in sequence" {
    const allocator = std.testing.allocator;
    var buf: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);

    // Write two packets
    var pkt1 = Packet{ .opcode = proto.OP_CREATE_EXTENT, .partition_id = 1, .req_id = 1 };
    var pkt2 = Packet{ .opcode = proto.OP_MARK_DELETE, .partition_id = 2, .req_id = 2 };
    try writePacket(fbs.writer(), &pkt1);
    try writePacket(fbs.writer(), &pkt2);

    // Read them back
    fbs.pos = 0;
    var d1 = try readPacket(fbs.reader(), allocator);
    defer d1.deinit();
    var d2 = try readPacket(fbs.reader(), allocator);
    defer d2.deinit();

    try std.testing.expectEqual(proto.OP_CREATE_EXTENT, d1.opcode);
    try std.testing.expectEqual(@as(i64, 1), d1.req_id);
    try std.testing.expectEqual(proto.OP_MARK_DELETE, d2.opcode);
    try std.testing.expectEqual(@as(i64, 2), d2.req_id);
}

test "codec: invalid magic returns error" {
    const allocator = std.testing.allocator;
    var buf: [proto.PACKET_HEADER_SIZE]u8 = undefined;
    @memset(&buf, 0);
    buf[0] = 0xAA; // wrong magic

    var fbs = std.io.fixedBufferStream(@as([]u8, &buf));
    const result = readPacket(fbs.reader(), allocator);
    try std.testing.expectError(error.InvalidMagic, result);
}
