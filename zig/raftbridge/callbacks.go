package main

// Callbacks wraps C function pointers into a Go StateMachine implementation.
// When tiglabs/raft commits a log entry, it calls Apply() on the StateMachine,
// which in turn calls the C function pointer to delegate back into Zig.

/*
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>

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

// CGo cannot call function pointers directly — we need C wrapper functions.

static inline int32_t call_apply(CStateMachineCallbacks *sm, const uint8_t *cmd, size_t cmd_len, uint64_t index) {
    if (sm && sm->apply_fn) {
        return sm->apply_fn(sm->ctx, cmd, cmd_len, index);
    }
    return -1;
}

static inline int32_t call_apply_member_change(CStateMachineCallbacks *sm, uint8_t change_type,
                                                uint64_t peer_id, const uint8_t *context,
                                                size_t context_len, uint64_t index) {
    if (sm && sm->apply_member_change_fn) {
        return sm->apply_member_change_fn(sm->ctx, change_type, peer_id, context, context_len, index);
    }
    return -1;
}

static inline uint64_t call_snapshot(CStateMachineCallbacks *sm) {
    if (sm && sm->snapshot_fn) {
        return sm->snapshot_fn(sm->ctx);
    }
    return 0;
}

static inline void call_handle_leader_change(CStateMachineCallbacks *sm, uint64_t leader_id) {
    if (sm && sm->handle_leader_change_fn) {
        sm->handle_leader_change_fn(sm->ctx, leader_id);
    }
}

static inline void call_handle_fatal(CStateMachineCallbacks *sm, const char *err_msg, size_t err_len) {
    if (sm && sm->handle_fatal_fn) {
        sm->handle_fatal_fn(sm->ctx, err_msg, err_len);
    }
}
*/
import "C"
import (
	"fmt"
	"unsafe"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
)

// bridgeStateMachine implements raft.StateMachine by delegating to C function pointers.
type bridgeStateMachine struct {
	callbacks *C.CStateMachineCallbacks
}

// newBridgeStateMachine creates a new bridge state machine from C callbacks.
// The callbacks pointer must remain valid for the lifetime of the state machine.
func newBridgeStateMachine(cb *C.CStateMachineCallbacks) *bridgeStateMachine {
	return &bridgeStateMachine{callbacks: cb}
}

// Apply is called when a committed log entry needs to be applied to the FSM.
func (sm *bridgeStateMachine) Apply(command []byte, index uint64) (interface{}, error) {
	if sm.callbacks == nil {
		return nil, fmt.Errorf("no callbacks registered")
	}

	var cmdPtr *C.uint8_t
	cmdLen := C.size_t(len(command))
	if len(command) > 0 {
		cmdPtr = (*C.uint8_t)(unsafe.Pointer(&command[0]))
	}

	rc := C.call_apply(sm.callbacks, cmdPtr, cmdLen, C.uint64_t(index))
	if int32(rc) != 0 {
		return nil, fmt.Errorf("apply failed at index %d: rc=%d", index, int32(rc))
	}
	return nil, nil
}

// ApplyMemberChange is called when a committed membership change needs to be applied.
func (sm *bridgeStateMachine) ApplyMemberChange(cc *proto.ConfChange, index uint64) (interface{}, error) {
	if sm.callbacks == nil {
		return nil, fmt.Errorf("no callbacks registered")
	}

	var ctxPtr *C.uint8_t
	ctxLen := C.size_t(len(cc.Context))
	if len(cc.Context) > 0 {
		ctxPtr = (*C.uint8_t)(unsafe.Pointer(&cc.Context[0]))
	}

	changeType := C.uint8_t(0)
	if cc.Type == proto.ConfRemoveNode {
		changeType = 1
	}

	rc := C.call_apply_member_change(sm.callbacks, changeType,
		C.uint64_t(cc.Peer.ID), ctxPtr, ctxLen, C.uint64_t(index))
	if int32(rc) != 0 {
		return nil, fmt.Errorf("apply member change failed at index %d: rc=%d", index, int32(rc))
	}
	return nil, nil
}

// Snapshot returns the current snapshot state. For the datanode, this is the applied index.
func (sm *bridgeStateMachine) Snapshot() (proto.Snapshot, error) {
	// The datanode doesn't use raft snapshots for data transfer — it has its own repair system.
	// Return an empty snapshot with the applied index as metadata.
	return nil, nil
}

// ApplySnapshot applies a snapshot received from the leader. No-op for datanode.
func (sm *bridgeStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	return nil
}

// HandleFatalEvent is called when a fatal raft error occurs.
func (sm *bridgeStateMachine) HandleFatalEvent(err *raft.FatalError) {
	if sm.callbacks == nil {
		return
	}
	msg := err.Error()
	cMsg := C.CString(msg)
	defer C.free(unsafe.Pointer(cMsg))
	C.call_handle_fatal(sm.callbacks, cMsg, C.size_t(len(msg)))
}

// HandleLeaderChange is called when the leader changes.
func (sm *bridgeStateMachine) HandleLeaderChange(leader uint64) {
	if sm.callbacks == nil {
		return
	}
	C.call_handle_leader_change(sm.callbacks, C.uint64_t(leader))
}
