const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create a module for the engine source
    const engine_mod = b.addModule("cubefs_engine", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Shared library: libcubefs_engine.so
    const lib = b.addSharedLibrary(.{
        .name = "cubefs_engine",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.linkLibC();
    lib.linkSystemLibrary("z");
    b.installArtifact(lib);

    // Static library: libcubefs_engine.a
    const static_lib = b.addStaticLibrary(.{
        .name = "cubefs_engine",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    static_lib.linkLibC();
    static_lib.linkSystemLibrary("z");
    b.installArtifact(static_lib);

    // Install the C header
    b.installFile("cubefs_engine.h", "include/cubefs_engine.h");

    // ── Datanode binary: cfs-datanode-zig ─────────────────────────
    const datanode_mod = b.addModule("cubefs_datanode", .{
        .root_source_file = b.path("src/datanode_main.zig"),
        .target = target,
        .optimize = optimize,
    });
    _ = datanode_mod;

    const datanode_exe = b.addExecutable(.{
        .name = "cfs-datanode-zig",
        .root_source_file = b.path("src/datanode_main.zig"),
        .target = target,
        .optimize = optimize,
    });
    datanode_exe.linkLibC();
    datanode_exe.linkSystemLibrary("z");
    b.installArtifact(datanode_exe);

    // ── Live E2E test binary ──────────────────────────────────────
    const e2e_exe = b.addExecutable(.{
        .name = "e2e-live-test",
        .root_source_file = b.path("src/e2e_live_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    e2e_exe.linkLibC();
    e2e_exe.linkSystemLibrary("z");
    b.installArtifact(e2e_exe);

    const e2e_repl_exe = b.addExecutable(.{
        .name = "e2e-live-replication-test",
        .root_source_file = b.path("src/e2e_live_replication_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    e2e_repl_exe.linkLibC();
    e2e_repl_exe.linkSystemLibrary("z");
    b.installArtifact(e2e_repl_exe);

    // ── Tests ─────────────────────────────────────────────────────

    // Unit tests (main.zig — FFI exports)
    const main_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    main_tests.linkLibC();
    main_tests.linkSystemLibrary("z");

    const run_main_tests = b.addRunArtifact(main_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_main_tests.step);

    // Existing test files (use engine module imports)
    const engine_test_files = [_][]const u8{
        "tests/extent_test.zig",
        "tests/extent_store_test.zig",
        "tests/cache_test.zig",
        "tests/sharded_map_test.zig",
        "tests/disk_partition_test.zig",
        "tests/handler_io_test.zig",
        "tests/admin_test.zig",
        "tests/io_uring_test.zig",
        "tests/replication_test.zig",
    };

    inline for (engine_test_files) |test_file| {
        const t = b.addTest(.{
            .root_source_file = b.path(test_file),
            .target = target,
            .optimize = optimize,
        });
        t.linkLibC();
        t.linkSystemLibrary("z");
        t.root_module.addImport("cubefs_engine", engine_mod);
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }

    // New source-level tests (embedded in src/*.zig files)
    const src_test_files = [_][]const u8{
        "src/protocol.zig",
        "src/packet.zig",
        "src/codec.zig",
        "src/qos.zig",
        "src/config.zig",
        "src/buffer_pool.zig",
        "src/conn_pool.zig",
        "src/replication.zig",
        "src/master_client.zig",
        "src/crc32.zig",
        "src/tiny_ring.zig",
        "src/types.zig",
        "src/error.zig",
        "src/raft.zig",
        "src/raft_log.zig",
        "src/raft_stub.zig",
        "src/metrics.zig",
        "src/partition_meta.zig",
        "src/shutdown.zig",
        "src/background.zig",
        "src/json.zig",
        "src/repair.zig",
        "src/connection.zig",
        "src/log.zig",
    };

    inline for (src_test_files) |test_file| {
        const t = b.addTest(.{
            .root_source_file = b.path(test_file),
            .target = target,
            .optimize = optimize,
        });
        t.linkLibC();
        t.linkSystemLibrary("z");
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }

    // Raft module tests (aggregated through a driver in src/ for correct import paths)
    {
        const t = b.addTest(.{
            .root_source_file = b.path("src/raft_test_driver.zig"),
            .target = target,
            .optimize = optimize,
        });
        t.linkLibC();
        t.linkSystemLibrary("z");
        const run_t = b.addRunArtifact(t);
        test_step.dependOn(&run_t.step);
    }

    // Functional tests (use datanode source imports from src/)
    {
        const t = b.addTest(.{
            .root_source_file = b.path("src/functional_test.zig"),
            .target = target,
            .optimize = optimize,
        });
        t.linkLibC();
        t.linkSystemLibrary("z");
        const run_t = b.addRunArtifact(t);
        const functest_step = b.step("functest", "Run functional tests");
        functest_step.dependOn(&run_t.step);
        test_step.dependOn(&run_t.step);
    }

    // ── Benchmark binary ─────────────────────────────────────────
    {
        const bench_exe = b.addExecutable(.{
            .name = "bench-extent-store",
            .root_source_file = b.path("tests/bench_extent_store.zig"),
            .target = target,
            .optimize = .ReleaseFast, // Always optimize benchmarks
        });
        bench_exe.linkLibC();
        bench_exe.linkSystemLibrary("z");
        bench_exe.root_module.addImport("cubefs_engine", engine_mod);
        b.installArtifact(bench_exe);

        const run_bench = b.addRunArtifact(bench_exe);
        const bench_step = b.step("bench", "Run ExtentStore benchmarks");
        bench_step.dependOn(&run_bench.step);
    }

    // ── Network benchmark binary ────────────────────────────────────
    {
        const net_bench_exe = b.addExecutable(.{
            .name = "bench-datanode-net",
            .root_source_file = b.path("tests/bench_datanode_net.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        });
        net_bench_exe.linkLibC();
        net_bench_exe.linkSystemLibrary("z");
        net_bench_exe.root_module.addImport("cubefs_engine", engine_mod);
        b.installArtifact(net_bench_exe);

        const run_net_bench = b.addRunArtifact(net_bench_exe);
        const net_bench_step = b.step("bench-net", "Run DataNode network benchmarks");
        net_bench_step.dependOn(&run_net_bench.step);
    }
}
