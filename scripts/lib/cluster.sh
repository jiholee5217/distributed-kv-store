#!/usr/bin/env bash

raft_nodes=(node-1 node-2 node-3 node-4 node-5)
raft_ports=(8081 8082 8083 8084 8085)

raft_now_ms() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

raft_find_leader() {
  local index status role
  for index in "${!raft_nodes[@]}"; do
    status=$(curl --silent --max-time 1 "http://127.0.0.1:${raft_ports[$index]}/v1/status" || true)
    role=$(python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("role", ""))
except Exception: print("")' <<<"$status")
    if [[ "$role" == "leader" ]]; then
      printf '%s\n' "${raft_nodes[$index]}"
      return 0
    fi
  done
  return 1
}

raft_port_for_node() {
  local index
  for index in "${!raft_nodes[@]}"; do
    if [[ "${raft_nodes[$index]}" == "$1" ]]; then
      printf '%s\n' "${raft_ports[$index]}"
      return 0
    fi
  done
  return 1
}

raft_first_node_except() {
  local node
  for node in "${raft_nodes[@]}"; do
    if [[ "$node" != "$1" ]]; then
      printf '%s\n' "$node"
      return 0
    fi
  done
  return 1
}

raft_wait_for_leader() {
  local excluded=${1:-} attempts=${2:-100} delay=${3:-0.1} candidate
  for _ in $(seq 1 "$attempts"); do
    if candidate=$(raft_find_leader) && [[ -n "$candidate" && "$candidate" != "$excluded" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
    sleep "$delay"
  done
  return 1
}
