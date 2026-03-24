// messages.zig — Binary message encode/decode for Raft RPCs.
//
// Wire format (fixed header + variable payload):
// +--------+--------+--------+--------+--------+-----------+--------------+---------+
// | Magic  | MsgType| Term   | From   | To     | PayloadLen| Partition ID | Payload |
// | 4 bytes| 1 byte | 8 bytes| 8 bytes| 8 bytes| 4 bytes   | 8 bytes      | N bytes |
// +--------+--------+--------+--------+--------+-----------+--------------+---------+
// Total header: 41 bytes

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_types = @import("types.zig");
const LogEntry = raft_types.LogEntry;
const EntryType = raft_types.EntryType;

/// Magic number identifying Raft RPC messages.
pub const RAFT_MSG_MAGIC: u32 = 0x52414654; // "RAFT"

/// Fixed header size before payload.
pub const HEADER_SIZE: usize = 41;

/// RPC message type.
pub const MsgType = enum(u8) {
    request_vote = 1,
    request_vote_response = 2,
    append_entries = 3,
    append_entries_response = 4,
    heartbeat = 5,
    heartbeat_response = 6,
};

/// A decoded Raft message.
pub const Message = struct {
    msg_type: MsgType,
    term: u64,
    from: u64,
    to: u64,
    partition_id: u64,
    payload: Payload,
};

/// Discriminated union of message payloads.
pub const Payload = union(enum) {
    request_vote: RequestVote,
    request_vote_response: RequestVoteResponse,
    append_entries: AppendEntries,
    append_entries_response: AppendEntriesResponse,
    heartbeat: Heartbeat,
    heartbeat_response: HeartbeatResponse,
};

pub const RequestVote = struct {
    last_log_index: u64,
    last_log_term: u64,
};

pub const RequestVoteResponse = struct {
    vote_granted: bool,
};

pub const AppendEntries = struct {
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    entries: []const LogEntry,
};

pub const AppendEntriesResponse = struct {
    success: bool,
    match_index: u64,
};

pub const Heartbeat = struct {
    leader_commit: u64,
};

pub const HeartbeatResponse = struct {
    match_index: u64,
};

// ─── Entry serialization ─────────────────────────────────────────

/// Per-entry header: term(8) + index(8) + type(1) + data_len(4) = 21 bytes.
const ENTRY_HEADER_SIZE: usize = 21;

/// Encode a slice of log entries into a byte buffer.
pub fn encodeEntries(allocator: Allocator, entries: []const LogEntry) ![]u8 {
    var total: usize = 0;
    for (entries) |e| {
        total += ENTRY_HEADER_SIZE + e.data.len;
    }

    const buf = try allocator.alloc(u8, total);
    var offset: usize = 0;

    for (entries) |e| {
        std.mem.writeInt(u64, buf[offset..][0..8], e.term, .big);
        offset += 8;
        std.mem.writeInt(u64, buf[offset..][0..8], e.index, .big);
        offset += 8;
        buf[offset] = @intFromEnum(e.entry_type);
        offset += 1;
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(e.data.len), .big);
        offset += 4;
        if (e.data.len > 0) {
            @memcpy(buf[offset .. offset + e.data.len], e.data);
            offset += e.data.len;
        }
    }

    return buf;
}

/// Decode a byte buffer into a slice of log entries.
/// The returned entry data slices point into the input buffer (zero-copy).
pub fn decodeEntries(allocator: Allocator, data: []const u8) ![]LogEntry {
    var entries = std.ArrayList(LogEntry).init(allocator);
    errdefer entries.deinit();

    var offset: usize = 0;
    while (offset + ENTRY_HEADER_SIZE <= data.len) {
        const term = std.mem.readInt(u64, data[offset..][0..8], .big);
        offset += 8;
        const index = std.mem.readInt(u64, data[offset..][0..8], .big);
        offset += 8;
        const entry_type: EntryType = @enumFromInt(data[offset]);
        offset += 1;
        const data_len = std.mem.readInt(u32, data[offset..][0..4], .big);
        offset += 4;

        if (offset + data_len > data.len) return error.InvalidData;

        const entry_data = data[offset .. offset + data_len];
        offset += data_len;

        try entries.append(.{
            .term = term,
            .index = index,
            .entry_type = entry_type,
            .data = entry_data,
        });
    }

    return entries.toOwnedSlice();
}

// ─── Message serialization ───────────────────────────────────────

/// Encode a Raft message into a byte buffer.
pub fn encode(allocator: Allocator, msg: *const Message) ![]u8 {
    const payload_bytes = try encodePayload(allocator, msg);
    defer if (payload_bytes.len > 0) allocator.free(payload_bytes);

    const total = HEADER_SIZE + payload_bytes.len;
    const buf = try allocator.alloc(u8, total);

    // Header
    std.mem.writeInt(u32, buf[0..4], RAFT_MSG_MAGIC, .big);
    buf[4] = @intFromEnum(msg.msg_type);
    std.mem.writeInt(u64, buf[5..13], msg.term, .big);
    std.mem.writeInt(u64, buf[13..21], msg.from, .big);
    std.mem.writeInt(u64, buf[21..29], msg.to, .big);
    std.mem.writeInt(u32, buf[29..33], @intCast(payload_bytes.len), .big);
    std.mem.writeInt(u64, buf[33..41], msg.partition_id, .big);

    // Payload
    if (payload_bytes.len > 0) {
        @memcpy(buf[HEADER_SIZE..], payload_bytes);
    }

    return buf;
}

/// Decode a Raft message from a byte buffer.
pub fn decode(allocator: Allocator, data: []const u8) !Message {
    if (data.len < HEADER_SIZE) return error.InvalidData;

    const magic = std.mem.readInt(u32, data[0..4], .big);
    if (magic != RAFT_MSG_MAGIC) return error.InvalidData;

    const msg_type: MsgType = @enumFromInt(data[4]);
    const term = std.mem.readInt(u64, data[5..13], .big);
    const from = std.mem.readInt(u64, data[13..21], .big);
    const to = std.mem.readInt(u64, data[21..29], .big);
    const payload_len = std.mem.readInt(u32, data[29..33], .big);
    const partition_id = std.mem.readInt(u64, data[33..41], .big);

    if (data.len < HEADER_SIZE + payload_len) return error.InvalidData;

    const payload_data = data[HEADER_SIZE .. HEADER_SIZE + payload_len];
    const payload = try decodePayload(allocator, msg_type, payload_data);

    return .{
        .msg_type = msg_type,
        .term = term,
        .from = from,
        .to = to,
        .partition_id = partition_id,
        .payload = payload,
    };
}

fn encodePayload(allocator: Allocator, msg: *const Message) ![]u8 {
    return switch (msg.payload) {
        .request_vote => |rv| blk: {
            const buf = try allocator.alloc(u8, 16);
            std.mem.writeInt(u64, buf[0..8], rv.last_log_index, .big);
            std.mem.writeInt(u64, buf[8..16], rv.last_log_term, .big);
            break :blk buf;
        },
        .request_vote_response => |rvr| blk: {
            const buf = try allocator.alloc(u8, 1);
            buf[0] = if (rvr.vote_granted) 1 else 0;
            break :blk buf;
        },
        .append_entries => |ae| blk: {
            const entries_bytes = try encodeEntries(allocator, ae.entries);
            defer if (entries_bytes.len > 0) allocator.free(entries_bytes);

            const buf = try allocator.alloc(u8, 28 + entries_bytes.len);
            std.mem.writeInt(u64, buf[0..8], ae.prev_log_index, .big);
            std.mem.writeInt(u64, buf[8..16], ae.prev_log_term, .big);
            std.mem.writeInt(u64, buf[16..24], ae.leader_commit, .big);
            std.mem.writeInt(u32, buf[24..28], @intCast(entries_bytes.len), .big);
            if (entries_bytes.len > 0) {
                @memcpy(buf[28..], entries_bytes);
            }
            break :blk buf;
        },
        .append_entries_response => |aer| blk: {
            const buf = try allocator.alloc(u8, 9);
            buf[0] = if (aer.success) 1 else 0;
            std.mem.writeInt(u64, buf[1..9], aer.match_index, .big);
            break :blk buf;
        },
        .heartbeat => |hb| blk: {
            const buf = try allocator.alloc(u8, 8);
            std.mem.writeInt(u64, buf[0..8], hb.leader_commit, .big);
            break :blk buf;
        },
        .heartbeat_response => |hbr| blk: {
            const buf = try allocator.alloc(u8, 8);
            std.mem.writeInt(u64, buf[0..8], hbr.match_index, .big);
            break :blk buf;
        },
    };
}

fn decodePayload(allocator: Allocator, msg_type: MsgType, data: []const u8) !Payload {
    return switch (msg_type) {
        .request_vote => blk: {
            if (data.len < 16) return error.InvalidData;
            break :blk Payload{ .request_vote = .{
                .last_log_index = std.mem.readInt(u64, data[0..8], .big),
                .last_log_term = std.mem.readInt(u64, data[8..16], .big),
            } };
        },
        .request_vote_response => blk: {
            if (data.len < 1) return error.InvalidData;
            break :blk Payload{ .request_vote_response = .{
                .vote_granted = data[0] != 0,
            } };
        },
        .append_entries => blk: {
            if (data.len < 28) return error.InvalidData;
            const prev_log_index = std.mem.readInt(u64, data[0..8], .big);
            const prev_log_term = std.mem.readInt(u64, data[8..16], .big);
            const leader_commit = std.mem.readInt(u64, data[16..24], .big);
            const entries_len = std.mem.readInt(u32, data[24..28], .big);
            const entries_data = data[28..];
            if (entries_data.len < entries_len) return error.InvalidData;

            const entries = try decodeEntries(allocator, entries_data[0..entries_len]);
            break :blk Payload{ .append_entries = .{
                .prev_log_index = prev_log_index,
                .prev_log_term = prev_log_term,
                .leader_commit = leader_commit,
                .entries = entries,
            } };
        },
        .append_entries_response => blk: {
            if (data.len < 9) return error.InvalidData;
            break :blk Payload{ .append_entries_response = .{
                .success = data[0] != 0,
                .match_index = std.mem.readInt(u64, data[1..9], .big),
            } };
        },
        .heartbeat => blk: {
            if (data.len < 8) return error.InvalidData;
            break :blk Payload{ .heartbeat = .{
                .leader_commit = std.mem.readInt(u64, data[0..8], .big),
            } };
        },
        .heartbeat_response => blk: {
            if (data.len < 8) return error.InvalidData;
            break :blk Payload{ .heartbeat_response = .{
                .match_index = std.mem.readInt(u64, data[0..8], .big),
            } };
        },
    };
}

// ─── Tests ──────────────────────────────────────────────────────────

const testing = std.testing;

test "encode/decode entries round-trip" {
    const alloc = testing.allocator;
    const entries = &[_]LogEntry{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = "hello" },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = "world" },
        .{ .term = 2, .index = 3, .entry_type = .noop, .data = &.{} },
    };

    const encoded = try encodeEntries(alloc, entries);
    defer alloc.free(encoded);

    const decoded = try decodeEntries(alloc, encoded);
    defer alloc.free(decoded);

    try testing.expectEqual(@as(usize, 3), decoded.len);
    try testing.expectEqual(@as(u64, 1), decoded[0].term);
    try testing.expectEqual(@as(u64, 1), decoded[0].index);
    try testing.expectEqualStrings("hello", decoded[0].data);
    try testing.expectEqual(@as(u64, 2), decoded[1].index);
    try testing.expectEqualStrings("world", decoded[1].data);
    try testing.expectEqual(EntryType.noop, decoded[2].entry_type);
    try testing.expectEqual(@as(usize, 0), decoded[2].data.len);
}

test "encode/decode RequestVote round-trip" {
    const alloc = testing.allocator;
    const msg = Message{
        .msg_type = .request_vote,
        .term = 5,
        .from = 1,
        .to = 2,
        .partition_id = 100,
        .payload = .{ .request_vote = .{ .last_log_index = 10, .last_log_term = 4 } },
    };

    const encoded = try encode(alloc, &msg);
    defer alloc.free(encoded);

    const decoded = try decode(alloc, encoded);
    try testing.expectEqual(MsgType.request_vote, decoded.msg_type);
    try testing.expectEqual(@as(u64, 5), decoded.term);
    try testing.expectEqual(@as(u64, 1), decoded.from);
    try testing.expectEqual(@as(u64, 2), decoded.to);
    try testing.expectEqual(@as(u64, 100), decoded.partition_id);
    try testing.expectEqual(@as(u64, 10), decoded.payload.request_vote.last_log_index);
    try testing.expectEqual(@as(u64, 4), decoded.payload.request_vote.last_log_term);
}

test "encode/decode AppendEntries round-trip" {
    const alloc = testing.allocator;
    const entries = &[_]LogEntry{
        .{ .term = 3, .index = 5, .data = "data1" },
    };
    const msg = Message{
        .msg_type = .append_entries,
        .term = 3,
        .from = 1,
        .to = 2,
        .partition_id = 42,
        .payload = .{ .append_entries = .{
            .prev_log_index = 4,
            .prev_log_term = 2,
            .leader_commit = 3,
            .entries = entries,
        } },
    };

    const encoded = try encode(alloc, &msg);
    defer alloc.free(encoded);

    const decoded = try decode(alloc, encoded);
    defer alloc.free(decoded.payload.append_entries.entries);

    try testing.expectEqual(MsgType.append_entries, decoded.msg_type);
    try testing.expectEqual(@as(u64, 4), decoded.payload.append_entries.prev_log_index);
    try testing.expectEqual(@as(u64, 2), decoded.payload.append_entries.prev_log_term);
    try testing.expectEqual(@as(u64, 3), decoded.payload.append_entries.leader_commit);
    try testing.expectEqual(@as(usize, 1), decoded.payload.append_entries.entries.len);
    try testing.expectEqualStrings("data1", decoded.payload.append_entries.entries[0].data);
}

test "encode/decode AppendEntriesResponse round-trip" {
    const alloc = testing.allocator;
    const msg = Message{
        .msg_type = .append_entries_response,
        .term = 3,
        .from = 2,
        .to = 1,
        .partition_id = 42,
        .payload = .{ .append_entries_response = .{ .success = true, .match_index = 10 } },
    };

    const encoded = try encode(alloc, &msg);
    defer alloc.free(encoded);

    const decoded = try decode(alloc, encoded);
    try testing.expect(decoded.payload.append_entries_response.success);
    try testing.expectEqual(@as(u64, 10), decoded.payload.append_entries_response.match_index);
}

test "encode/decode Heartbeat round-trip" {
    const alloc = testing.allocator;
    const msg = Message{
        .msg_type = .heartbeat,
        .term = 7,
        .from = 1,
        .to = 3,
        .partition_id = 99,
        .payload = .{ .heartbeat = .{ .leader_commit = 50 } },
    };

    const encoded = try encode(alloc, &msg);
    defer alloc.free(encoded);

    const decoded = try decode(alloc, encoded);
    try testing.expectEqual(MsgType.heartbeat, decoded.msg_type);
    try testing.expectEqual(@as(u64, 50), decoded.payload.heartbeat.leader_commit);
}

test "decode rejects short buffer" {
    const alloc = testing.allocator;
    const short = [_]u8{ 0, 0, 0 };
    try testing.expectError(error.InvalidData, decode(alloc, &short));
}

test "decode rejects bad magic" {
    const alloc = testing.allocator;
    var buf: [HEADER_SIZE]u8 = undefined;
    @memset(&buf, 0);
    std.mem.writeInt(u32, buf[0..4], 0xDEADBEEF, .big);
    try testing.expectError(error.InvalidData, decode(alloc, &buf));
}
