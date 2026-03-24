package main

// CubeFS Raft Bridge — Go shared library wrapping tiglabs/raft for Zig FFI.
//
// Build: go build -buildmode=c-shared -o libcubefs_raft.so
// This produces libcubefs_raft.so + libcubefs_raft.h (auto-generated).

/*
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/storage/wal"
)

// raftServerWrapper holds the tiglabs/raft server and per-partition state machines.
type raftServerWrapper struct {
	mu     sync.RWMutex
	server *raft.RaftServer

	// Per-partition state machines, indexed by partition ID.
	stateMachines map[uint64]*bridgeStateMachine

	// Node resolver: maps node_id -> address info.
	nodeResolver *nodeResolver

	// Server configuration for reference.
	nodeID uint64
}

// nodeResolver implements raft.NodeResolver for the tiglabs/raft transport.
type nodeResolver struct {
	mu    sync.RWMutex
	nodes map[uint64]*nodeInfo
}

type nodeInfo struct {
	addr          string
	heartbeatPort int
	replicaPort   int
}

func newNodeResolver() *nodeResolver {
	return &nodeResolver{
		nodes: make(map[uint64]*nodeInfo),
	}
}

func (r *nodeResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.nodes[nodeID]
	if !ok {
		return "", fmt.Errorf("node %d not found", nodeID)
	}
	switch stype {
	case raft.HeartBeat:
		return fmt.Sprintf("%s:%d", info.addr, info.heartbeatPort), nil
	case raft.Replicate:
		return fmt.Sprintf("%s:%d", info.addr, info.replicaPort), nil
	default:
		return fmt.Sprintf("%s:%d", info.addr, info.heartbeatPort), nil
	}
}

func (r *nodeResolver) addNode(nodeID uint64, addr string, heartbeatPort, replicaPort int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[nodeID] = &nodeInfo{
		addr:          addr,
		heartbeatPort: heartbeatPort,
		replicaPort:   replicaPort,
	}
}

func (r *nodeResolver) removeNode(nodeID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
}

// ─── Exported C Functions ─────────────────────────────────────────

//export cubefs_raft_server_new
func cubefs_raft_server_new(config *C.CRaftConfig) unsafe.Pointer {
	if config == nil {
		log.Println("[raft-bridge] cubefs_raft_server_new: nil config")
		return nil
	}

	nodeID := uint64(config.node_id)
	raftPath := C.GoString(config.raft_path)
	heartbeatAddr := C.GoString(config.heartbeat_addr)
	replicaAddr := C.GoString(config.replica_addr)
	tickInterval := int(config.tick_interval_ms)
	electionTick := int(config.election_tick)
	retainLogs := uint64(config.retain_logs)

	// Ensure raft directory exists
	if err := os.MkdirAll(raftPath, 0755); err != nil {
		log.Printf("[raft-bridge] failed to create raft dir %s: %v", raftPath, err)
		return nil
	}

	resolver := newNodeResolver()

	rc := raft.DefaultConfig()
	rc.NodeID = nodeID
	rc.TickInterval = time.Duration(tickInterval) * time.Millisecond
	rc.ElectionTick = electionTick
	rc.HeartbeatAddr = heartbeatAddr
	rc.ReplicateAddr = replicaAddr
	rc.Resolver = resolver
	rc.RetainLogs = retainLogs

	server, err := raft.NewRaftServer(rc)
	if err != nil {
		log.Printf("[raft-bridge] failed to create raft server: %v", err)
		return nil
	}

	wrapper := &raftServerWrapper{
		server:        server,
		stateMachines: make(map[uint64]*bridgeStateMachine),
		nodeResolver:  resolver,
		nodeID:        nodeID,
	}

	log.Printf("[raft-bridge] raft server created: node_id=%d, heartbeat=%s, replica=%s",
		nodeID, heartbeatAddr, replicaAddr)

	return unsafe.Pointer(wrapper)
}

//export cubefs_raft_server_stop
func cubefs_raft_server_stop(serverPtr unsafe.Pointer) {
	if serverPtr == nil {
		return
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	if wrapper.server != nil {
		wrapper.server.Stop()
		wrapper.server = nil
		log.Printf("[raft-bridge] raft server stopped: node_id=%d", wrapper.nodeID)
	}
}

//export cubefs_raft_server_add_node
func cubefs_raft_server_add_node(serverPtr unsafe.Pointer, nodeID C.uint64_t,
	addr *C.char, heartbeatPort C.int, replicaPort C.int) {
	if serverPtr == nil {
		return
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	goAddr := C.GoString(addr)
	wrapper.nodeResolver.addNode(uint64(nodeID), goAddr, int(heartbeatPort), int(replicaPort))
}

//export cubefs_raft_server_remove_node
func cubefs_raft_server_remove_node(serverPtr unsafe.Pointer, nodeID C.uint64_t) {
	if serverPtr == nil {
		return
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.nodeResolver.removeNode(uint64(nodeID))
}

//export cubefs_raft_partition_create
func cubefs_raft_partition_create(serverPtr unsafe.Pointer, id C.uint64_t,
	peers *C.CPeer, peerCount C.size_t, applied C.uint64_t,
	walPath *C.char, sm *C.CStateMachineCallbacks) C.int32_t {
	if serverPtr == nil {
		return C.int32_t(errInvalidArg)
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	if wrapper.server == nil {
		return C.int32_t(errRaftStopped)
	}

	partitionID := uint64(id)
	goPeers := convertCPeers(peers, peerCount)
	goWalPath := C.GoString(walPath)

	// Create WAL storage for this partition
	walDir := filepath.Join(goWalPath, fmt.Sprintf("wal_%d", partitionID))
	if err := os.MkdirAll(walDir, 0755); err != nil {
		log.Printf("[raft-bridge] failed to create WAL dir %s: %v", walDir, err)
		return C.int32_t(errInternal)
	}

	walStorage, err := wal.NewStorage(walDir, nil)
	if err != nil {
		log.Printf("[raft-bridge] failed to create WAL storage for partition %d: %v",
			partitionID, err)
		return C.int32_t(errInternal)
	}

	// Create bridge state machine from C callbacks
	bridgeSM := newBridgeStateMachine(sm)

	raftConfig := &raft.RaftConfig{
		ID:      partitionID,
		Applied: uint64(applied),
		Peers:   goPeers,
		Storage: walStorage,
		StateMachine: bridgeSM,
	}

	if err := wrapper.server.CreateRaft(raftConfig); err != nil {
		log.Printf("[raft-bridge] failed to create raft partition %d: %v", partitionID, err)
		walStorage.Close()
		return C.int32_t(errInternal)
	}

	wrapper.stateMachines[partitionID] = bridgeSM
	log.Printf("[raft-bridge] created raft partition %d with %d peers, applied=%d",
		partitionID, len(goPeers), uint64(applied))
	return C.int32_t(errOK)
}

//export cubefs_raft_partition_stop
func cubefs_raft_partition_stop(serverPtr unsafe.Pointer, id C.uint64_t) C.int32_t {
	if serverPtr == nil {
		return C.int32_t(errInvalidArg)
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	if wrapper.server == nil {
		return C.int32_t(errRaftStopped)
	}

	partitionID := uint64(id)
	if err := wrapper.server.RemoveRaft(partitionID); err != nil {
		log.Printf("[raft-bridge] failed to stop raft partition %d: %v", partitionID, err)
		return C.int32_t(errInternal)
	}

	delete(wrapper.stateMachines, partitionID)
	return C.int32_t(errOK)
}

//export cubefs_raft_partition_delete
func cubefs_raft_partition_delete(serverPtr unsafe.Pointer, id C.uint64_t) C.int32_t {
	// Stop first, then the caller is responsible for deleting WAL files
	return cubefs_raft_partition_stop(serverPtr, id)
}

//export cubefs_raft_submit
func cubefs_raft_submit(serverPtr unsafe.Pointer, partitionID C.uint64_t,
	cmd *C.uint8_t, cmdLen C.size_t) C.int32_t {
	if serverPtr == nil {
		return C.int32_t(errInvalidArg)
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.RLock()
	server := wrapper.server
	wrapper.mu.RUnlock()

	if server == nil {
		return C.int32_t(errRaftStopped)
	}

	// Copy command data from C to Go
	goCmd := C.GoBytes(unsafe.Pointer(cmd), C.int(cmdLen))

	future := server.Submit(uint64(partitionID), goCmd)
	_, err := future.Response()
	if err != nil {
		if err == raft.ErrNotLeader {
			return C.int32_t(errNotLeader)
		}
		if err == raft.ErrStopped {
			return C.int32_t(errRaftStopped)
		}
		return C.int32_t(errRaftSubmitFail)
	}

	return C.int32_t(errOK)
}

//export cubefs_raft_change_member
func cubefs_raft_change_member(serverPtr unsafe.Pointer, partitionID C.uint64_t,
	changeType C.uint8_t, peerID C.uint64_t,
	context *C.uint8_t, contextLen C.size_t) C.int32_t {
	if serverPtr == nil {
		return C.int32_t(errInvalidArg)
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.RLock()
	server := wrapper.server
	wrapper.mu.RUnlock()

	if server == nil {
		return C.int32_t(errRaftStopped)
	}

	peer := proto.Peer{ID: uint64(peerID)}
	ct := confChangeType(changeType)

	var goContext []byte
	if contextLen > 0 && context != nil {
		goContext = C.GoBytes(unsafe.Pointer(context), C.int(contextLen))
	}

	future := server.ChangeMember(uint64(partitionID), ct, peer, goContext)
	_, err := future.Response()
	if err != nil {
		if err == raft.ErrNotLeader {
			return C.int32_t(errNotLeader)
		}
		return C.int32_t(errRaftSubmitFail)
	}

	return C.int32_t(errOK)
}

//export cubefs_raft_is_leader
func cubefs_raft_is_leader(serverPtr unsafe.Pointer, partitionID C.uint64_t) C.bool {
	if serverPtr == nil {
		return C.bool(false)
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.RLock()
	server := wrapper.server
	wrapper.mu.RUnlock()

	if server == nil {
		return C.bool(false)
	}

	return C.bool(server.IsLeader(uint64(partitionID)))
}

//export cubefs_raft_applied_index
func cubefs_raft_applied_index(serverPtr unsafe.Pointer, partitionID C.uint64_t) C.uint64_t {
	if serverPtr == nil {
		return 0
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.RLock()
	server := wrapper.server
	wrapper.mu.RUnlock()

	if server == nil {
		return 0
	}

	return C.uint64_t(server.AppliedIndex(uint64(partitionID)))
}

//export cubefs_raft_leader_term
func cubefs_raft_leader_term(serverPtr unsafe.Pointer, partitionID C.uint64_t,
	leaderOut *C.uint64_t, termOut *C.uint64_t) {
	if serverPtr == nil || leaderOut == nil || termOut == nil {
		return
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.RLock()
	server := wrapper.server
	wrapper.mu.RUnlock()

	if server == nil {
		*leaderOut = 0
		*termOut = 0
		return
	}

	leader, term := server.LeaderTerm(uint64(partitionID))
	*leaderOut = C.uint64_t(leader)
	*termOut = C.uint64_t(term)
}

//export cubefs_raft_truncate
func cubefs_raft_truncate(serverPtr unsafe.Pointer, partitionID C.uint64_t, index C.uint64_t) {
	if serverPtr == nil {
		return
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.RLock()
	server := wrapper.server
	wrapper.mu.RUnlock()

	if server == nil {
		return
	}

	server.Truncate(uint64(partitionID), uint64(index))
}

//export cubefs_raft_try_to_leader
func cubefs_raft_try_to_leader(serverPtr unsafe.Pointer, partitionID C.uint64_t) C.int32_t {
	if serverPtr == nil {
		return C.int32_t(errInvalidArg)
	}
	wrapper := (*raftServerWrapper)(serverPtr)
	wrapper.mu.RLock()
	server := wrapper.server
	wrapper.mu.RUnlock()

	if server == nil {
		return C.int32_t(errRaftStopped)
	}

	if err := server.TryToLeader(uint64(partitionID)); err != nil {
		return C.int32_t(errInternal)
	}
	return C.int32_t(errOK)
}

// main is required for c-shared buildmode but is never called.
func main() {}
