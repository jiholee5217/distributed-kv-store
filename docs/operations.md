# Running and observing the Raft cluster

## Start the complete topology

```bash
docker compose up --detach --build
```

This starts five Raft nodes with separate persistent volumes, Prometheus, and a
provisioned Grafana dashboard.

| Surface | URL | Purpose |
| --- | --- | --- |
| Node APIs | `http://127.0.0.1:8081` through `:8085` | CRUD, status, health, and raw metrics |
| Prometheus | <http://127.0.0.1:9090> | Queries, target health, and metric inspection |
| Grafana | <http://127.0.0.1:3000> | Raft role, term, lag, latency, elections, RPC failures, and log growth |

## Inspect the current leader

```bash
for port in 8081 8082 8083 8084 8085; do
  curl -sS "http://127.0.0.1:${port}/v1/status"
  echo
done
```

Exactly one healthy node should report `"role":"leader"`. Commit indexes may
briefly differ while a follower receives the next heartbeat.

## Useful Prometheus queries

```promql
sum(raft_kv_node_role{role="leader"})
max(raft_kv_node_commit_index) - raft_kv_node_commit_index
histogram_quantile(0.95, sum by (le, node) (rate(raft_kv_node_proposal_duration_seconds_bucket{outcome="committed"}[1m])))
sum by (node, outcome) (rate(raft_kv_node_peer_rpc_total{outcome!="success"}[1m]))
sum by (method, status) (rate(raft_kv_http_requests_total{route="/v1/kv/{key}"}[1m]))
```

## Run a steady workload

```bash
./scripts/benchmark.sh
```

The script discovers the leader and targets a follower so the workload exercises
normal forwarding. Configuration is controlled through environment variables:

```bash
CONCURRENCY=32 DURATION=15s WORKLOAD=mixed KEYSPACE=64 VALUE_BYTES=64 \
  JSON_OUTPUT=1 ./scripts/benchmark.sh
```

Supported workloads are `mixed` (50/50 PUT and linearizable GET), `write`, and
`read`. Read and mixed runs pre-seed the configured keyspace outside the measured
window.

## Inject leader failure

Run a correctness-oriented demonstration:

```bash
./scripts/failover-demo.sh
```

It discovers the leader, commits a value, stops that leader, times replacement,
commits another value, restarts the failed node, and waits for its commit index
to catch up.

Run a leader failure during sustained writes:

```bash
./scripts/failover-load.sh
```

The load target is a surviving follower. The report separates successful
throughput from HTTP 503 and transport errors during the election window. The
script restores the stopped leader even if the experiment exits early.

## Restart and recovery checks

After a node restarts, compare its status with the leader:

```bash
curl -sS http://127.0.0.1:8081/v1/status
curl -sS http://127.0.0.1:8082/v1/status
```

The restarted follower loads its persisted term, vote, log, and commit index,
then accepts the leader's missing suffix. A zero commit-index lag across nodes is
the clearest catch-up signal currently exported.

## Stop safely

```bash
docker compose down
```

This preserves the five Raft volumes. Use `docker compose down --volumes` only
when you intentionally want to erase all persisted cluster state.

## Troubleshooting

- If no leader appears, verify at least three nodes are running and mutually
  reachable, then inspect `docker compose logs` and election metrics.
- A 503 during election is expected; use bounded retry backoff instead of a tight
  retry loop.
- Rising proposal latency and log size are expected with the current full-log
  JSON persistence. Reset only disposable test volumes or implement WAL/snapshot
  milestones before long benchmarks.
