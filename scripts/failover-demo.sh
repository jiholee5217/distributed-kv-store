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

echo "initial leader: $leader"
curl --silent --fail --request PUT http://127.0.0.1:8081/v1/kv/failover-demo \
  --header 'Content-Type: application/json' \
  --data '{"value":"before"}' >/dev/null

started=$(raft_now_ms)
docker compose stop "$leader" >/dev/null
failed_leader=$leader

if ! replacement=$(raft_wait_for_leader "$leader" 200 0.05); then
  echo "cluster did not replace the failed leader" >&2
  exit 1
fi
elapsed=$(( $(raft_now_ms) - started ))
echo "replacement leader: $replacement (${elapsed} ms)"

replacement_port=$(raft_port_for_node "$replacement")
curl --silent --fail --request PUT "http://127.0.0.1:${replacement_port}/v1/kv/failover-demo" \
  --header 'Content-Type: application/json' \
  --data '{"value":"after"}' >/dev/null

docker compose start "$leader" >/dev/null
failed_leader=""
restarted_port=$(raft_port_for_node "$leader")
for _ in $(seq 1 100); do
  restarted_status=$(curl --silent --max-time 1 "http://127.0.0.1:${restarted_port}/v1/status" || true)
  restarted_commit=$(python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("commit_index", 0))
except Exception: print(0)' <<<"$restarted_status")
  replacement_status=$(curl --silent --max-time 1 "http://127.0.0.1:${replacement_port}/v1/status" || true)
  replacement_commit=$(python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("commit_index", 0))
except Exception: print(0)' <<<"$replacement_status")
  if [[ "$restarted_commit" -ge "$replacement_commit" && "$replacement_commit" -gt 0 ]]; then
    break
  fi
  sleep 0.05
done
if [[ "$restarted_commit" -lt "$replacement_commit" || "$replacement_commit" -eq 0 ]]; then
  echo "restarted node did not catch up before the deadline" >&2
  exit 1
fi
echo "restarted node caught up: $leader (commit_index=$restarted_commit)"
curl --silent --fail "http://127.0.0.1:${restarted_port}/v1/kv/failover-demo"
echo
