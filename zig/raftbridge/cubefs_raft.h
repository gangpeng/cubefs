// cubefs_raft.h — C header for the Go raft bridge library.
// This header is used by both the Go cgo layer and the Zig FFI bindings.

#ifndef CUBEFS_RAFT_H
#define CUBEFS_RAFT_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ─── Opaque types ─────────────────────────────────────────────────

typedef struct CRaftServer CRaftServer;

// ─── Configuration ────────────────────────────────────────────────

typedef struct CRaftConfig {
    uint64_t node_id;
    const char *raft_path;
    const char *heartbeat_addr;
    const char *replica_addr;
    int tick_interval_ms;
    int election_tick;
    uint64_t retain_logs;
} CRaftConfig;

typedef struct CPeer {
    uint64_t id;
    const char *address;
    int heartbeat_port;
    int replica_port;
} CPeer;

// ─── State Machine Callbacks ──────────────────────────────────────
//
// The Go bridge calls these C function pointers to delegate FSM
// operations back into Zig (or any other language).

typedef struct CStateMachineCallbacks {
    void *ctx;

    // Apply a committed log entry. Returns 0 on success, non-zero on error.
    int32_t (*apply_fn)(void *ctx, const uint8_t *cmd, size_t cmd_len, uint64_t index);

    // Apply a committed membership change.
    int32_t (*apply_member_change_fn)(void *ctx, uint8_t change_type,
                                      uint64_t peer_id, const uint8_t *context,
                                      size_t context_len, uint64_t index);

    // Return the last applied snapshot index.
    uint64_t (*snapshot_fn)(void *ctx);

    // Called when the leader changes.
    void (*handle_leader_change_fn)(void *ctx, uint64_t leader_id);

    // Called on fatal raft errors.
    void (*handle_fatal_fn)(void *ctx, const char *err_msg, size_t err_len);
} CStateMachineCallbacks;

// ─── Server lifecycle ─────────────────────────────────────────────

// Create a new raft server. Returns NULL on error.
extern CRaftServer *cubefs_raft_server_new(CRaftConfig *config);

// Stop and destroy the raft server.
extern void cubefs_raft_server_stop(CRaftServer *server);

// Register a node address for the raft transport resolver.
extern void cubefs_raft_server_add_node(CRaftServer *server, uint64_t node_id,
                                         const char *addr, int heartbeat_port,
                                         int replica_port);

// Remove a node from the resolver.
extern void cubefs_raft_server_remove_node(CRaftServer *server, uint64_t node_id);

// ─── Partition operations ─────────────────────────────────────────

// Create a raft partition (replica group).
// Returns 0 on success, non-zero error code on failure.
extern int32_t cubefs_raft_partition_create(CRaftServer *server, uint64_t id,
                                             const CPeer *peers, size_t peer_count,
                                             uint64_t applied, const char *wal_path,
                                             CStateMachineCallbacks *sm);

// Stop a raft partition.
extern int32_t cubefs_raft_partition_stop(CRaftServer *server, uint64_t id);

// Stop and delete a raft partition (removes WAL data).
extern int32_t cubefs_raft_partition_delete(CRaftServer *server, uint64_t id);

// Submit a command through raft consensus.
// Blocks until the command is committed or an error occurs.
// Returns 0 on success, non-zero error code on failure.
extern int32_t cubefs_raft_submit(CRaftServer *server, uint64_t partition_id,
                                   const uint8_t *cmd, size_t cmd_len);

// Submit a membership change through raft consensus.
// change_type: 0 = add_node, 1 = remove_node
extern int32_t cubefs_raft_change_member(CRaftServer *server, uint64_t partition_id,
                                          uint8_t change_type, uint64_t peer_id,
                                          const uint8_t *context, size_t context_len);

// Check if this node is the leader for the given partition.
extern bool cubefs_raft_is_leader(CRaftServer *server, uint64_t partition_id);

// Get the last applied index for the given partition.
extern uint64_t cubefs_raft_applied_index(CRaftServer *server, uint64_t partition_id);

// Get the current leader ID and term for the given partition.
// leader_out and term_out must be valid pointers.
extern void cubefs_raft_leader_term(CRaftServer *server, uint64_t partition_id,
                                     uint64_t *leader_out, uint64_t *term_out);

// Truncate the WAL for a partition up to the given index.
extern void cubefs_raft_truncate(CRaftServer *server, uint64_t partition_id,
                                  uint64_t index);

// Attempt leadership transfer for the given partition.
extern int32_t cubefs_raft_try_to_leader(CRaftServer *server, uint64_t partition_id);

#ifdef __cplusplus
}
#endif

#endif // CUBEFS_RAFT_H
