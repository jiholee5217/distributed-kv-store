#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
source "$repo_root/scripts/lib/cluster.sh"

failed_leader=""
cleanup() {
  if [[ -n "$failed_leader" ]]; then
    docker compose start "$failed_leader" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker compose up --detach --build
if ! leader=$(raft_wait_for_leader); then
  echo "cluster did not elect a leader" >&2
  exit 1
fi
target_node=$(raft_first_node_except "$leader")
target_port=$(raft_port_for_node "$target_node")
benchmark_binary=/tmp/distributed-kv-failover-benchmark
result_file=$(mktemp /tmp/distributed-kv-failover.XXXXXX.json)
trap 'rm -f "$result_file"; cleanup' EXIT
GOCACHE=${GOCACHE:-/tmp/distributed-kv-go-cache} go build -o "$benchmark_binary" ./cmd/kvbench

echo "leader before fault: $leader"
echo "load target: $target_node"
"$benchmark_binary" \
  -target "http://127.0.0.1:${target_port}" \
  -concurrency "${CONCURRENCY:-32}" \
  -duration "${DURATION:-15s}" \
  -workload write \
  -keyspace "${KEYSPACE:-64}" \
  -value-bytes "${VALUE_BYTES:-64}" \
  -json >"$result_file" &
benchmark_pid=$!

sleep "${FAIL_AFTER_SECONDS:-5}"
started=$(raft_now_ms)
docker compose stop "$leader" >/dev/null
failed_leader=$leader
if ! replacement=$(raft_wait_for_leader "$leader" 200 0.05); then
  echo "cluster did not replace the failed leader" >&2
  wait "$benchmark_pid" || true
  exit 1
fi
recovery_ms=$(( $(raft_now_ms) - started ))
wait "$benchmark_pid"

echo "replacement leader: $replacement (${recovery_ms} ms)"
cat "$result_file"
docker compose start "$leader" >/dev/null
failed_leader=""
