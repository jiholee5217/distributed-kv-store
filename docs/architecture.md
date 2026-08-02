# System design

Editable diagram: [distributed-kv-store-architecture.excalidraw](distributed-kv-store-architecture.excalidraw)

## Goal

The system exposes a small `key -> value` API while keeping every healthy node
on the same committed history. It favors correctness and inspectability over
production-scale optimization.

The deployment uses five statically configured nodes. At any moment each node
is a follower, candidate, or leader. A majority of three nodes is required to
elect a leader and commit a log entry.

## Components

| Component | Responsibility |
| --- | --- |
| HTTP API | Accept client operations and forward follower requests |
| Raft node | Own terms, roles, votes, timers, log replication, and commit rules |
| RPC transport | Exchange `RequestVote` and `AppendEntries` messages over HTTP |
| Persistent storage | Atomically save term, vote, log, and commit index |
| State machine | Apply committed `PUT`, `DELETE`, and read-barrier commands |
| Docker Compose | Run an isolated five-node network with one volume per node |
| Benchmark client | Generate concurrent traffic and calculate latency percentiles |
| Fault demo | Stop the elected leader and measure replacement-election time |

## Write path

```mermaid
sequenceDiagram
    participant C as Client
    participant F as Any node
    participant L as Leader
    participant A as Follower A
    participant B as Follower B
    participant S as State machine

    C->>F: PUT /v1/kv/key
    alt F is not leader
        F->>L: Forward request
    end
    L->>L: Append entry and persist
    par Replicate
        L->>A: AppendEntries
        L->>B: AppendEntries
    end
    A-->>L: Persisted acknowledgement
    B-->>L: Persisted acknowledgement
    L->>L: Majority reached; advance commitIndex
    L->>S: Apply committed command
    L-->>C: 201 Created
    L->>A: Next heartbeat carries leaderCommit
    L->>B: Next heartbeat carries leaderCommit
    A->>A: Apply committed command
    B->>B: Apply committed command
```

The client receives success only after the entry is stored on a majority and
applied by the leader. A follower learns the updated commit index through the
next `AppendEntries` request and applies the same commands in the same order.

## Leader election

Followers reset a randomized election deadline whenever they receive a valid
leader heartbeat or grant a vote. If the deadline expires, a follower:

1. increments its term and becomes a candidate;
2. votes for itself and persists that vote;
3. asks every peer for a vote;
4. becomes leader after receiving a majority; and
5. appends a no-op entry in its new term and begins heartbeats.

A voter grants at most one vote per term and rejects a candidate whose log is
less up to date than its own. Randomized deadlines reduce repeated split votes.
Any node that observes a higher term immediately becomes a follower.

## Log replication and safety

Every entry contains a monotonically increasing index, the leader term that
created it, and a deterministic state-machine command. `AppendEntries` includes
the index and term immediately before the proposed entries. A follower accepts
the request only when that prefix matches its own log; otherwise the leader
backs up to the follower's reported conflict index and retries.

The leader advances `commitIndex` only when:

- a majority has stored the candidate index; and
- the entry at that index was created in the leader's current term.

Applying commands strictly between `lastApplied + 1` and `commitIndex` keeps
the state machine aligned with the committed log.

## Reads

A leader can become isolated without immediately knowing that a newer leader
exists. Returning its local map directly could therefore serve stale data.

This implementation uses a simple linearizable read barrier: each `GET` appends
and commits a no-op entry before reading the state machine. That proves the
leader can still contact a majority in its current term. It is correct but more
expensive than Raft's optimized `ReadIndex` protocol, which is a future
performance improvement.

## Persistence and recovery

Before acknowledging safety-critical transitions, a node atomically persists:

- `currentTerm`;
- `votedFor`;
- the replicated log; and
- `commitIndex`.

The implementation writes a temporary JSON state file, calls `fsync`, and
renames it over the previous file. On restart the node validates log indexes and
replays every entry through the persisted commit index. Uncommitted suffixes
remain in the log and are reconciled by the next leader.

Rewriting the full log is intentionally transparent but eventually expensive.
A segmented checksummed WAL plus periodic snapshots is the intended next
storage layer.

## Failure behavior

| Live, mutually reachable nodes | Expected behavior |
| --- | --- |
| 5 | Normal operation |
| 4 | Normal operation after any one crash |
| 3 | Still able to elect and commit |
| 2 or fewer | No writes or linearizable reads; majority unavailable |

During leader failure there is a short period with no leader. Requests return
`503 Service Unavailable` until an election completes. Followers then learn the
replacement leader from heartbeats and resume forwarding.

## Trust boundaries and limitations

- Membership is static and configured identically on every node.
- Peer endpoints assume a trusted private network; there is no mTLS yet.
- The failure model is crash-stop or network loss, not Byzantine behavior.
- There is no snapshot installation, log compaction, or dynamic membership.
- Forwarding is one hop and guarded against loops.
- Performance results must include the machine, Docker version, workload,
  commit, duration, and raw benchmark output.
