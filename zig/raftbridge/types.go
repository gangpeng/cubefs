package main

// Types and conversion helpers for the C FFI bridge.

/*
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct CPeer {
    uint64_t id;
    const char *address;
    int heartbeat_port;
    int replica_port;
} CPeer;

typedef struct CStateMachineCallbacks {
    void *ctx;
    int32_t (*apply_fn)(void *ctx, const uint8_t *cmd, size_t cmd_len, uint64_t index);
    int32_t (*apply_member_change_fn)(void *ctx, uint8_t change_type,
                                      uint64_t peer_id, const uint8_t *context,
                                      size_t context_len, uint64_t index);
    uint64_t (*snapshot_fn)(void *ctx);
    void (*handle_leader_change_fn)(void *ctx, uint64_t leader_id);
    void (*handle_fatal_fn)(void *ctx, const char *err_msg, size_t err_len);
} CStateMachineCallbacks;
*/
import "C"
import (
	"unsafe"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
)

// Error codes matching the Zig error module.
const (
	errOK              int32 = 0
	errInvalidArg      int32 = -1
	errNotFound        int32 = -2
	errNotLeader       int32 = -3
	errRaftStopped     int32 = -4
	errRaftSubmitFail  int32 = -5
	errInternal        int32 = -6
	errRaftExists      int32 = -7
)

// convertCPeers converts a C array of CPeer to a Go slice of proto.Peer.
func convertCPeers(cpeers *C.CPeer, count C.size_t) []proto.Peer {
	n := int(count)
	if n == 0 || cpeers == nil {
		return nil
	}

	// Build a Go slice from the C array without copying the C array itself
	cSlice := unsafe.Slice(cpeers, n)
	peers := make([]proto.Peer, n)
	for i := 0; i < n; i++ {
		cp := cSlice[i]
		peers[i] = proto.Peer{
			ID: uint64(cp.id),
		}
	}
	return peers
}

// confChangeType converts a C uint8 to a proto.ConfChangeType.
func confChangeType(ct C.uint8_t) proto.ConfChangeType {
	switch uint8(ct) {
	case 0:
		return proto.ConfAddNode
	case 1:
		return proto.ConfRemoveNode
	default:
		return proto.ConfAddNode
	}
}
