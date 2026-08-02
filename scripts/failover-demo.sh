#!/usr/bin/env bash
set -euo pipefail

nodes=(node-1 node-2 node-3 node-4 node-5)
ports=(8081 8082 8083 8084 8085)

now_ms() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

find_leader() {
  local index status role
  for index in "${!nodes[@]}"; do
    status="$(curl --silent --max-time 1 "http://127.0.0.1:${ports[$index]}/v1/status" || true)"
    role="$(python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("role", ""))
except Exception: print("")' <<<"$status")"
    if [[ "$role" == "leader" ]]; then
      printf '%s\n' "${nodes[$index]}"
      return 0
    fi
  done
  return 1
}

port_for_node() {
  local index
  for index in "${!nodes[@]}"; do
    if [[ "${nodes[$index]}" == "$1" ]]; then
      printf '%s\n' "${ports[$index]}"
      return 0
    fi
  done
  return 1
}

docker compose up --detach --build

leader=""
for _ in {1..100}; do
  if leader="$(find_leader)"; then
    break
  fi
  sleep 0.1
done
if [[ -z "$leader" ]]; then
  echo "cluster did not elect a leader" >&2
  exit 1
fi

echo "initial leader: $leader"
curl --silent --fail --request PUT http://127.0.0.1:8081/v1/kv/failover-demo \
  --header 'Content-Type: application/json' \
  --data '{"value":"before"}' >/dev/null

started="$(now_ms)"
docker compose stop "$leader" >/dev/null

replacement=""
for _ in {1..100}; do
  if candidate="$(find_leader)" && [[ "$candidate" != "$leader" ]]; then
    replacement="$candidate"
    break
  fi
  sleep 0.05
done
if [[ -z "$replacement" ]]; then
  echo "cluster did not replace the failed leader" >&2
  exit 1
fi
elapsed=$(( $(now_ms) - started ))
echo "replacement leader: $replacement (${elapsed} ms)"

replacement_port="$(port_for_node "$replacement")"
curl --silent --fail --request PUT "http://127.0.0.1:${replacement_port}/v1/kv/failover-demo" \
  --header 'Content-Type: application/json' \
  --data '{"value":"after"}' >/dev/null

docker compose start "$leader" >/dev/null
sleep 1
curl --silent --fail http://127.0.0.1:8081/v1/kv/failover-demo
echo
