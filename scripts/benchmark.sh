#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
source "$repo_root/scripts/lib/cluster.sh"

docker compose up --detach --build
if ! leader=$(raft_wait_for_leader); then
  echo "cluster did not elect a leader" >&2
  exit 1
fi

target_node=${TARGET_NODE:-$(raft_first_node_except "$leader")}
target_port=$(raft_port_for_node "$target_node")
echo "leader: $leader"
echo "benchmark target: $target_node (requests exercise follower forwarding)"

benchmark_binary=/tmp/distributed-kv-benchmark
GOCACHE=${GOCACHE:-/tmp/distributed-kv-go-cache} go build -o "$benchmark_binary" ./cmd/kvbench
"$benchmark_binary" \
  -target "http://127.0.0.1:${target_port}" \
  -concurrency "${CONCURRENCY:-32}" \
  -duration "${DURATION:-15s}" \
  -workload "${WORKLOAD:-mixed}" \
  -keyspace "${KEYSPACE:-64}" \
  -value-bytes "${VALUE_BYTES:-64}" \
  ${JSON_OUTPUT:+-json}
