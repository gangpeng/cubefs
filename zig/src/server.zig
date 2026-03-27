// server.zig — TCP listener + accept loop for the Zig datanode.

const std = @import("std");
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const config_mod = @import("config.zig");
const disk_mod = @import("disk.zig");
const space_mgr_mod = @import("space_manager.zig");
const master_client_mod = @import("master_client.zig");
const replication_mod = @import("replication.zig");
const handler_mod = @import("handler.zig");
const connection_mod = @import("connection.zig");
const admin_mod = @import("admin.zig");
const json_mod = @import("json.zig");
const raft_server_mod = @import("raft/server.zig");
const proto = @import("protocol.zig");
const log = @import("log.zig");
const Config = config_mod.Config;
const Disk = disk_mod.Disk;
const SpaceManager = space_mgr_mod.SpaceManager;
const MasterClient = master_client_mod.MasterClient;
const ReplicationPipeline = replication_mod.ReplicationPipeline;
const ServerInner = handler_mod.ServerInner;
const RaftServer = raft_server_mod.RaftServer;

const SEND_BUFFER_SIZE: u32 = 2 * 1024 * 1024; // 2 MB — must exceed max response size (1 MB reads)
const HEARTBEAT_INTERVAL_NS: u64 = 10 * std.time.ns_per_s;

/// Context passed to background threads that need server state.
const HeartbeatContext = struct {
    master_client: *MasterClient,
    space_mgr: *SpaceManager,
    start_time: i64,
    zone_name: []const u8,
    local_addr: []const u8,
    port: u16,
    allocator: Allocator,
};

/// Main server entry point. Runs until process termination.
pub fn run(config: Config, allocator: Allocator) !void {
    std.debug.print("[server] run() entered\n", .{});
    const start_time = std.time.timestamp();

    // Initialize space manager
    var space_mgr = SpaceManager.init(allocator);
    std.debug.print("[server] space_mgr initialized\n", .{});

    // Initialize disks
    const disk_entries = try config.parseDiskEntries(allocator);
    defer allocator.free(disk_entries);
    std.debug.print("[server] parsed {d} disk entries\n", .{disk_entries.len});

    for (disk_entries) |entry| {
        std.debug.print("[server] initializing disk: {s}\n", .{entry.path});
        const d = Disk.init(allocator, entry.path, entry.reserved_space, 0, 0) catch |e| {
            log.err("failed to initialize disk {s}: {}", .{ entry.path, e });
            continue;
        };
        space_mgr.addDisk(d) catch continue;
        log.info("initialized disk: {s} (total={d}, avail={d})", .{
            d.path,
            d.getTotal(),
            d.getAvailable(),
        });
    }

    // Initialize master client
    var master_client = MasterClient.init(
        allocator,
        config.master_addr,
        config.local_ip,
        config.port,
        config.zone_name,
        config.raft_heartbeat,
        config.raft_replica,
    );
    std.debug.print("[server] master_client initialized\n", .{});

    // Initialize replication pipeline
    var replication = ReplicationPipeline.init(allocator);
    std.debug.print("[server] replication initialized\n", .{});

    // Initialize Raft server (only if node_id is configured)
    var raft_server: ?*RaftServer = null;
    if (config.node_id != 0) {
        const election_timeout_ms = config.raft_tick_interval * config.raft_election_tick;
        raft_server = RaftServer.init(allocator, .{
            .node_id = config.node_id,
            .heartbeat_addr = config.local_ip,
            .heartbeat_port = config.raft_heartbeat,
            .tick_interval_ms = config.raft_tick_interval,
            .wal_dir = config.raft_dir,
            .election_timeout_ms = election_timeout_ms,
            .retain_logs = config.raft_retain_logs,
        }) catch |e| blk: {
            log.err("failed to initialize raft server: {}", .{e});
            break :blk null;
        };
    }

    // Create server inner
    var server = ServerInner.init(
        allocator,
        &space_mgr,
        &master_client,
        &replication,
        raft_server,
        config.local_ip,
        config.port,
    );

    // Start replication workers (must be after struct is at final location)
    replication.start();

    // Load existing partitions from disk
    server.loadPartitions();
    log.info("loaded {d} partitions", .{space_mgr.partitionCount()});

    // Start Raft server and register partitions
    if (raft_server) |rs| {
        rs.start() catch |e| {
            log.err("failed to start raft server: {}", .{e});
        };

        // Start raft for each loaded partition
        const partitions = space_mgr.allPartitions(allocator) catch &.{};
        defer allocator.free(partitions);

        for (partitions) |dp| {
            dp.startRaftWithServer(rs);
        }

        log.info("raft server started: node_id={d}", .{config.node_id});
    }

    // Spawn heartbeat background thread with full context
    const hb_ctx = allocator.create(HeartbeatContext) catch null;
    if (hb_ctx) |ctx| {
        ctx.* = .{
            .master_client = &master_client,
            .space_mgr = &space_mgr,
            .start_time = start_time,
            .zone_name = config.zone_name,
            .local_addr = config.local_ip,
            .port = config.port,
            .allocator = allocator,
        };
        _ = std.Thread.spawn(.{}, heartbeatLoop, .{ctx}) catch |e| {
            log.warn("failed to spawn heartbeat thread: {}", .{e});
        };
    }

    // Spawn admin HTTP server thread with start_time
    const admin_port = config.port +| 1;
    _ = std.Thread.spawn(.{}, admin_mod.startAdminServer, .{ &space_mgr, config.local_ip, admin_port, start_time, allocator }) catch |e| {
        log.warn("failed to spawn admin server: {}", .{e});
    };

    // TCP accept loop
    std.debug.print("[server] binding to {s}:{d}\n", .{ config.local_ip, config.port });
    const address = try net.Address.parseIp4(config.local_ip, config.port);
    var tcp_server = try address.listen(.{
        .reuse_address = true,
    });
    defer tcp_server.deinit();

    std.debug.print("[server] listening, entering accept loop\n", .{});
    log.info("datanode listening on {s}:{d}", .{ config.local_ip, config.port });

    while (true) {
        const conn = tcp_server.accept() catch |e| {
            log.warn("accept error: {}", .{e});
            continue;
        };

        // Set TCP_NODELAY
        setNoDelay(conn.stream) catch {};

        // Set send buffer size (2 MB) — must exceed max response to avoid
        // writev blocking on send buffer drain for 1 MB read responses.
        setSendBuf(conn.stream, SEND_BUFFER_SIZE) catch {};

        // Spawn connection handler thread
        _ = std.Thread.spawn(.{}, connection_mod.handleConnection, .{ conn.stream, &server, allocator }) catch |e| {
            log.warn("failed to spawn connection thread: {}", .{e});
            conn.stream.close();
        };
    }
}

fn heartbeatLoop(ctx: *HeartbeatContext) void {
    // First call: register
    ctx.master_client.register() catch |e| {
        log.warn("initial registration failed: {}", .{e});
    };

    while (true) {
        std.time.sleep(HEARTBEAT_INTERVAL_NS);

        // Build heartbeat response with disk stats + partition reports
        const body = buildHeartbeatBody(ctx) catch |e| {
            log.warn("failed to build heartbeat body: {}", .{e});
            // Fall back to simple registration
            ctx.master_client.register() catch |e2| {
                log.warn("heartbeat registration failed: {}", .{e2});
            };
            continue;
        };
        defer ctx.allocator.free(body);

        ctx.master_client.sendHeartbeat(body) catch |e| {
            log.warn("heartbeat send failed: {}", .{e});
        };
    }
}

fn buildHeartbeatBody(ctx: *HeartbeatContext) ![]u8 {
    const allocator = ctx.allocator;

    // Gather disk stats
    const disk_count = ctx.space_mgr.diskCount();
    var disk_stats = std.ArrayList(json_mod.DiskStat).init(allocator);
    defer disk_stats.deinit();

    var all_disks = std.ArrayList([]const u8).init(allocator);
    defer all_disks.deinit();

    var total_space: u64 = 0;
    var total_used: u64 = 0;
    var total_available: u64 = 0;
    var bad_disks = std.ArrayList([]const u8).init(allocator);
    defer bad_disks.deinit();

    var i: usize = 0;
    while (i < disk_count) : (i += 1) {
        if (ctx.space_mgr.getDisk(i)) |d| {
            // Refresh space stats
            d.updateSpace() catch {};

            const dt = d.getTotal();
            const du = d.getUsed();
            const da = d.getAvailable();
            total_space += dt;
            total_used += du;
            total_available += da;

            try all_disks.append(d.path);

            try disk_stats.append(.{
                .DiskPath = d.path,
                .Total = dt,
                .Used = du,
                .Available = da,
                .Status = d.status.load(.acquire),
            });

            if (!d.isGood()) {
                try bad_disks.append(d.path);
            }
        }
    }

    // Gather partition reports
    const partitions = try ctx.space_mgr.allPartitions(allocator);
    defer allocator.free(partitions);

    var reports = std.ArrayList(json_mod.DataPartitionReport).init(allocator);
    defer reports.deinit();

    var total_partition_size: u64 = 0;

    for (partitions) |dp| {
        const used = dp.usedSize();
        total_partition_size += dp.getSize();

        try reports.append(.{
            .PartitionID = dp.partition_id,
            .PartitionStatus = dp.getStatus(),
            .Total = dp.getSize(),
            .Used = used,
            .DiskPath = dp.disk.path,
            .ExtentCount = @intCast(dp.extentCount()),
        });
    }

    // Build operator address: "ip:port"
    var addr_buf: [256]u8 = undefined;
    const operator_addr = std.fmt.bufPrint(&addr_buf, "{s}:{d}", .{ ctx.local_addr, ctx.port }) catch "unknown";

    const heartbeat = json_mod.HeartbeatResponse{
        .Total = total_space,
        .Used = total_used,
        .Available = total_available,
        .RemainingCapacity = total_available,
        .MaxCapacity = total_available,
        .TotalPartitionSize = total_partition_size,
        .CreatedPartitionCnt = @intCast(partitions.len),
        .ZoneName = ctx.zone_name,
        .PartitionReports = reports.items,
        .AllDisks = all_disks.items,
        .DiskStats = disk_stats.items,
        .BadDisks = bad_disks.items,
        .StartTime = ctx.start_time,
        .Status = 1, // TaskSucceeds
        .Result = "success",
    };

    // Wrap in AdminTask envelope — the master expects this format
    const task_resp = json_mod.HeartbeatTaskResponse{
        .OpCode = proto.OP_DATA_NODE_HEARTBEAT,
        .OperatorAddr = operator_addr,
        .Status = 1, // TaskSucceeds
        .SendTime = std.time.timestamp(),
        .Response = heartbeat,
    };

    return try json_mod.stringify(allocator, task_resp);
}

fn setNoDelay(stream: net.Stream) !void {
    const fd = stream.handle;
    const rc = posix.system.setsockopt(
        fd,
        posix.system.IPPROTO.TCP,
        std.posix.system.TCP.NODELAY,
        &std.mem.toBytes(@as(c_int, 1)),
        @sizeOf(c_int),
    );
    if (rc != 0) return error.SetSockOptFailed;
}

fn setSendBuf(stream: net.Stream, size: u32) !void {
    const fd = stream.handle;
    const rc = posix.system.setsockopt(
        fd,
        posix.system.SOL.SOCKET,
        posix.system.SO.SNDBUF,
        &std.mem.toBytes(@as(c_int, @intCast(size))),
        @sizeOf(c_int),
    );
    if (rc != 0) return error.SetSockOptFailed;
}
