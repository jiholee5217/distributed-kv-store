# Codebase guide

```text
.
├── cmd/
│   ├── kvnode/               # Node process and CLI configuration
│   └── kvbench/              # Concurrent benchmark client
├── internal/
│   ├── raft/
│   │   ├── node.go           # Elections, replication, commits, recovery
│   │   ├── http.go           # Client API, forwarding, and Raft RPC handlers
│   │   ├── storage.go        # File and in-memory persistent-state backends
│   │   ├── metrics.go        # Bounded-cardinality Prometheus instrumentation
│   │   ├── types.go          # Log, RPC, member, role, and status types
│   │   └── *_test.go         # Safety, persistence, and five-node tests
│   └── statemachine/
│       └── machine.go        # Deterministic committed key-value state
├── scripts/
│   ├── benchmark.sh          # Reproducible steady workload
│   ├── failover-demo.sh      # Leader crash, write, restart, and catch-up
│   └── failover-load.sh      # Sustained writes during leader failure
├── deploy/                   # Prometheus and Grafana provisioning
├── docker-compose.yml        # Five nodes, five volumes, metrics, and dashboard
├── Dockerfile                # Minimal production node image
└── docs/
    ├── architecture.md       # Protocol and system-design decisions
    └── codebase.md           # This map
```

## How the pieces interact

### `cmd/kvnode`

Parses node identity, address, static membership, timer, and data-directory
flags. It creates file-backed storage and a private Prometheus registry,
constructs the Raft node, exposes `/metrics`, starts timers, and manages graceful
HTTP shutdown.

### `internal/raft/node.go`

This is the consensus engine. Its mutex-protected state contains the role,
term, vote, log, commit progress, known leader, and per-follower replication
indexes. The major flows are:

- `startElection`: advance the term, request votes, and become leader;
- `HandleRequestVote`: enforce one vote per term and the up-to-date-log rule;
- `replicatePeer`: send the follower the missing log suffix;
- `HandleAppendEntries`: validate the prefix, reconcile conflicts, and commit;
- `advanceCommitLocked`: detect majority replication in the current term; and
- `Propose`: append a client command and wait until it is committed.

### `internal/raft/http.go`

Separates transport behavior from consensus state. Public requests arriving on
a follower are proxied once to the known leader. Internal endpoints decode
Raft RPCs and call the consensus engine. The status endpoint exposes enough
state to observe elections and replication without reading private fields.

### `internal/raft/storage.go`

Defines a small `Storage` interface. `FileStorage` is used by real nodes and
performs atomic durable replacement. `MemoryStorage` makes deterministic tests
fast without weakening the production path.

### `internal/statemachine`

Contains no networking or consensus logic. It knows only how to apply an
ordered command. Keeping it deterministic means every node that applies the
same committed log reaches the same key-value state.

### Tests and operational tooling

The integration test launches five real HTTP servers, waits for one leader,
writes through a follower, verifies replication, kills the leader, waits for a
replacement, writes again, then removes enough nodes to prove a minority cannot
advance the commit index. Focused tests cover stale voting, conflicting suffix
repair, restart replay, persistence, state-machine behavior, and metric updates.

Docker Compose provides the human-observable topology. The benchmark records
workload shape, successes, error categories, throughput, and percentiles. The
failure scripts measure both correctness-oriented restart catch-up and request
behavior during an under-load election.
