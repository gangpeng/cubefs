#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_ROOT=${TMP_ROOT:-/tmp/cubefs_zig_live_single}
CONFIG_PATH="$TMP_ROOT/datanode.json"
DATA_PORT=${DATA_PORT:-17410}
LOG_PREFIX='[single-node]'
DATANODE_PID=''

cleanup() {
  if [[ -n "$DATANODE_PID" ]] && kill -0 "$DATANODE_PID" 2>/dev/null; then
    kill "$DATANODE_PID" 2>/dev/null || true
    wait "$DATANODE_PID" 2>/dev/null || true
  fi
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
mkdir -p "$TMP_ROOT/disk0/datapartition_10001" "$TMP_ROOT/raft" "$TMP_ROOT/log"

cat >"$CONFIG_PATH" <<EOF
{
  "local_ip": "127.0.0.1",
  "port": $DATA_PORT,
  "master_addr": [],
  "disks": ["$TMP_ROOT/disk0"],
  "raft_dir": "$TMP_ROOT/raft",
  "raft_heartbeat": 17330,
  "raft_replica": 17340,
  "zone_name": "test",
  "log_dir": "$TMP_ROOT/log",
  "log_level": "info"
}
EOF

cd "$ROOT_DIR"

echo "$LOG_PREFIX building Zig artifacts"
zig build

echo "$LOG_PREFIX starting cfs-datanode-zig on 127.0.0.1:$DATA_PORT"
./zig-out/bin/cfs-datanode-zig --config "$CONFIG_PATH" >"$TMP_ROOT/datanode.stdout.log" 2>"$TMP_ROOT/datanode.stderr.log" &
DATANODE_PID=$!

wait_for_port "$DATA_PORT"
wait_for_port "$((DATA_PORT + 1))"

echo "$LOG_PREFIX running live single-node E2E"
./zig-out/bin/e2e-live-test

echo "$LOG_PREFIX running functional test target"
zig build functest

echo "$LOG_PREFIX running full Zig test target"
zig build test

echo "$LOG_PREFIX completed successfully"