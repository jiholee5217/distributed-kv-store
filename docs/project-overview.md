# Project overview for reviewers

## One-sentence version

This project is a from-scratch five-node Raft database that demonstrates leader
election, majority-backed writes, linearizable reads, persistent restart recovery,
quorum-loss safety, and measurable leader-failure behavior.

## Sixty-second explanation

A client can send `PUT`, `GET`, or `DELETE` to any of five HTTP servers. Raft
elects one server as leader; followers remember that leader and forward client
traffic to it. For a write, the leader appends a command to its persistent log
and sends the missing suffix to every follower. Once three of five nodes have
stored the entry, the leader marks it committed, applies it to the in-memory
key-value state machine, and responds. Followers learn the commit index from
later heartbeats and apply the same operations in the same order.

If the leader crashes, requests can fail while there is no leader. Randomized
timeouts trigger a new election, and only a candidate with a sufficiently
up-to-date log can receive votes. The replacement resumes commits once it reaches
a majority. A restarted node loads its persisted state and reconciles its suffix
with the current leader.

## Five-minute walkthrough

1. Start with the [diagram gallery](diagrams.md) for the system, write, election,
   quorum, and recovery paths.
2. Read [architecture](architecture.md) for the safety invariants implemented in
   `RequestVote`, `AppendEntries`, and commit advancement.
3. Inspect [`internal/raft/node.go`](../internal/raft/node.go) for the consensus
   engine and [`internal/raft/http.go`](../internal/raft/http.go) for transport.
4. Review [client semantics](client-semantics.md) for linearizable reads, 503s,
   timeouts, and why retries are not exactly once.
5. Review the [recorded results](results/2026-08-15-five-node.md) and reproduce
   them with the scripts in [`scripts`](../scripts).

## Engineering decisions worth discussing

| Decision | Reason | Tradeoff |
| --- | --- | --- |
| Implement Raft directly | Makes terms, votes, conflicts, and commit rules inspectable | Educational implementation lacks production hardening |
| Require three of five replicas | Survives two crash-stop failures without divergent commits | Minority partitions reject writes and linearizable reads |
| Commit a barrier for every GET | Simple proof that the leader still reaches a majority | Reads pay replication and persistence latency and grow the log |
| Rewrite a versioned, checksummed JSON state file atomically | Recovery format is easy to inspect and corruption is detected before replay | Cost rises with every log entry; no segmented WAL or compaction |
| Forward through any node | Simple client endpoint behavior | Adds a proxy hop and exposes a short 503 window during elections |
| Export bounded-cardinality metrics | Elections and replication are observable without key-level labels | Metrics do not replace tracing or durable audit events |

## Evidence ledger

| Invariant or behavior | Evidence |
| --- | --- |
| One elected leader and majority-backed replication | Five-node HTTP integration test under `go test -race` |
| A minority cannot commit | Same test removes three nodes and verifies `commitIndex` does not advance |
| Stale candidates cannot win | `TestRequestVoteRejectsStaleCandidate` |
| Conflicting suffixes converge | `TestAppendEntriesReplacesConflictingSuffix` |
| Committed state replays | Memory/file restart tests, checksum-corruption tests, legacy-format loading, and Docker restart catch-up demo |
| Operational signals are exported | Metrics unit test, five Prometheus targets, provisioned Grafana dashboard |
| Failure cost is measured | No-load and under-load leader-stop experiments with recorded recovery windows |

## Measured behavior

The controlled 32-client mixed workload completed 4,174 operations in 15 seconds
through a follower with zero errors. Stopping the leader without load produced a
replacement in 763 ms. During saturated write load, replacement took 2.144
seconds and clients observed a temporary 32.8% error rate; after restart, all five
commit indexes converged.

Those numbers are useful precisely because they expose the current design:
consensus safety holds, but full-log rewrites and missing client retries make the
election window visible. They are not production database claims.

## Interview prompts

- Why can a five-node cluster lose two nodes but not three?
- Why may a successful-looking local append still be unsafe to acknowledge?
- How does the up-to-date-log voting rule protect committed entries?
- Why does Raft restrict commit advancement for older-term entries?
- What can a client infer after receiving 503 versus losing a connection?
- Why does a read barrier prevent an isolated old leader from serving stale data?
- Which metrics reveal split votes, lagging followers, or storage bottlenecks?
- What changes first: segmented WAL, snapshots, `ReadIndex`, or Multi-Raft, and why?
