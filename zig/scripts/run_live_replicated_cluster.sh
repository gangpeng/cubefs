#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_ROOT=${TMP_ROOT:-/tmp/cubefs_zig_live_cluster}
HOST=${HOST:-127.0.0.1}
PORTS=(${PORTS:-17410 17420 17430})
RAFT_HEARTBEATS=(${RAFT_HEARTBEATS:-17330 17350 17370})
RAFT_REPLICAS=(${RAFT_REPLICAS:-17340 17360 17380})
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
}

wait_for_port() {
  local port=$1
  local attempts=${2:-60}
  for ((i = 0; i < attempts; i += 1)); do
    if ss -ltn | grep -q ":$port "; then
      return 0
    fi
    sleep 1
  done
  return 1
}

trap cleanup EXIT

rm -rf "$TMP_ROOT"
mkdir -p "$TMP_ROOT"

cd "$ROOT_DIR"

echo '[cluster] building Zig artifacts'
zig build

for idx in 0 1 2; do
  node_dir="$TMP_ROOT/node$idx"
  mkdir -p "$node_dir/disk0/datapartition_10001" "$node_dir/raft" "$node_dir/log"

  cat >"$node_dir/datanode.json" <<EOF
{
  "local_ip": "$HOST",
  "port": ${PORTS[$idx]},
  "master_addr": [],
  "disks": ["$node_dir/disk0"],
  "raft_dir": "$node_dir/raft",
  "raft_heartbeat": ${RAFT_HEARTBEATS[$idx]},
  "raft_replica": ${RAFT_REPLICAS[$idx]},
  "zone_name": "test",
  "log_dir": "$node_dir/log",
  "log_level": "info"
}
EOF

  echo "[cluster] starting node$idx on $HOST:${PORTS[$idx]}"
  ./zig-out/bin/cfs-datanode-zig --config "$node_dir/datanode.json" >"$node_dir/datanode.stdout.log" 2>"$node_dir/datanode.stderr.log" &
  PIDS+=("$!")
done

for idx in 0 1 2; do
  wait_for_port "${PORTS[$idx]}"
  wait_for_port "$((PORTS[$idx] + 1))"
done

echo '[cluster] running live replication client'
./zig-out/bin/e2e-live-replication-test --host "$HOST" --leader-port "${PORTS[0]}" --followers "${PORTS[1]},${PORTS[2]}"

echo '[cluster] completed successfully'