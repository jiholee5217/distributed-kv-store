# Project overview

## One-sentence version

This project is a five-node, persistent key-value database that uses Raft to
keep an ordered command log consistent, remain available through two node
failures, and recover committed data after restarts.

## Sixty-second explanation

A client can send `PUT`, `GET`, or `DELETE` to any of five HTTP servers. Raft
elects one server as leader; followers remember that leader and forward client
traffic to it. For a write, the leader appends a command to its persistent log
and sends the missing suffix to every follower. Once three of five nodes have
stored the entry, the leader marks it committed, applies it to the in-memory
key-value state machine, and responds to the client. Followers learn the commit
index from later heartbeats and apply the same operations in the same order.

If the leader crashes, heartbeats stop. After randomized election timeouts, a
follower becomes a candidate, increments the term, and requests votes. A
majority elects a replacement whose log is sufficiently up to date. Requests
resume after the new leader begins heartbeats. When a crashed node restarts, it
loads its persisted log and catches up from the current leader.

## Major parts

1. **Consensus engine** — roles, terms, randomized elections, voting, log
   matching, replication progress, and majority commit decisions.
2. **Persistent log** — durable term, vote, entries, and commit index with
   atomic replacement and restart replay.
3. **Key-value state machine** — deterministic application of committed puts,
   deletes, and no-op read barriers.
4. **HTTP transport** — public CRUD API, one-hop follower forwarding, and
   internal `RequestVote`/`AppendEntries` RPCs.
5. **Five-node environment** — Docker network, isolated processes, and one
   durable volume per node.
6. **Verification** — race tests, a real five-server integration test, leader
   crash injection, persistence restart checks, and a concurrent benchmark.

## Consistency and availability

- Writes are acknowledged only after majority persistence and leader apply.
- Reads use a committed no-op barrier, so an isolated old leader cannot serve a
  stale value as current.
- A five-node cluster can tolerate two crash-stop failures and still form a
  majority.
- With fewer than three mutually reachable nodes, the system sacrifices
  availability rather than accepting divergent writes.

## What makes the project technically interesting

The hard part is not the map that stores values. It is maintaining invariants
while elections, retries, conflicting log suffixes, crashes, and concurrent
client requests happen:

- a node votes at most once per term;
- only a candidate with an up-to-date log can win;
- a follower accepts entries only after a matching prefix;
- only majority-replicated current-term entries advance the leader's commit
  index; and
- state-machine commands apply once, in committed log order.

## Current engineering baseline

The current persistence layer rewrites and `fsync`s the complete JSON log for
clarity, and every read commits a log barrier for simple linearizability. These
choices are correct but deliberately expensive. The benchmark harness makes
that cost visible and creates a concrete path to a segmented WAL, batching,
snapshots, and an optimized `ReadIndex` implementation.

Performance numbers should be reported only with the exact commit, workload,
machine, duration, and raw benchmark output. Hypothetical résumé numbers are
not project results.
