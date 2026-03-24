// datanode_main.zig — Binary entry point for the Zig CubeFS datanode.
// Usage: cfs-datanode-zig --config <path>

const std = @import("std");
const config_mod = @import("config.zig");
const server_mod = @import("server.zig");
const shutdown_mod = @import("shutdown.zig");
const log = @import("log.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Install signal handlers for graceful shutdown
    shutdown_mod.installSignalHandlers();

    // Parse CLI args
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var config_path: ?[]const u8 = null;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--config") or std.mem.eql(u8, args[i], "-c")) {
            if (i + 1 < args.len) {
                i += 1;
                config_path = args[i];
            }
        } else if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            printUsage();
            return;
        }
    }

    const path = config_path orelse {
        std.debug.print("error: --config <path> is required\n", .{});
        printUsage();
        std.process.exit(1);
    };

    // Load config
    const config = config_mod.Config.fromFile(allocator, path) catch |e| {
        std.debug.print("error: failed to load config from {s}: {}\n", .{ path, e });
        std.process.exit(1);
    };

    // Set log level
    if (std.mem.eql(u8, config.log_level, "debug")) {
        log.setLevel(.debug);
    } else if (std.mem.eql(u8, config.log_level, "warn")) {
        log.setLevel(.warn);
    } else if (std.mem.eql(u8, config.log_level, "error")) {
        log.setLevel(.err);
    } else {
        log.setLevel(.info);
    }

    log.info("CubeFS Zig DataNode starting...", .{});
    log.info("  config: {s}", .{path});
    log.info("  bind:   {s}:{d}", .{ config.local_ip, config.port });
    log.info("  disks:  {d}", .{config.disks.len});
    if (config.node_id != 0) {
        log.info("  raft:   node_id={d}, heartbeat={d}", .{ config.node_id, config.raft_heartbeat });
    }

    // Run server (blocks forever)
    server_mod.run(config, allocator) catch |e| {
        log.err("server failed: {}", .{e});
        std.debug.print("FATAL: server exited with error: {}\n", .{e});
        std.process.exit(1);
    };
}

fn printUsage() void {
    std.debug.print(
        \\Usage: cfs-datanode-zig [options]
        \\
        \\Options:
        \\  --config, -c <path>  Path to JSON configuration file (required)
        \\  --help, -h           Show this help message
        \\
    , .{});
}
