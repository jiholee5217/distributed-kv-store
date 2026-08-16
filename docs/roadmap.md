# Roadmap

Each milestone requires a correctness test, an operational demonstration, and an
honest update to the public claims.

## Implemented baseline

- [x] Randomized elections, term changes, and one vote per term
- [x] Up-to-date-log voting rule
- [x] `AppendEntries` heartbeats, prefix matching, and suffix repair
- [x] Majority commit restricted to current-term entries
- [x] Ordered deterministic state-machine application
- [x] Linearizable reads through committed barriers
- [x] Persistent term, vote, log, and commit-index replay
- [x] Five-node Docker cluster, leader failure, restart catch-up, and quorum-loss test
- [x] Prometheus metrics, Grafana dashboard, benchmark, and under-load fault script

## Storage durability and bounded growth

- [ ] Segmented write-ahead log with checksums and explicit record framing
- [ ] Batched `fsync` with a documented durability/latency policy
- [ ] Parent-directory `fsync` and crash-point tests around atomic replacement
- [ ] State-machine snapshots and log compaction
- [ ] `InstallSnapshot` catch-up for followers behind the compacted prefix

Exit condition: long workloads have bounded recovery time and disk growth, and
crash tests validate every persistence boundary.

## Protocol and client hardening

- [ ] Optimized `ReadIndex` path that does not append per read
- [ ] Durable request IDs and deduplicated retry results
- [ ] Structured leader hints and an official retrying client
- [ ] mTLS and authorization for peer/client endpoints
- [ ] Network-partition matrix with repeated election and healing tests

Exit condition: retry and security semantics are explicit, tested, and observable.

## Membership and scale

- [ ] Joint-consensus membership changes
- [ ] Learner nodes and controlled promotion
- [ ] Sharding or Multi-Raft for horizontal write throughput
- [ ] Placement/rebalancing policy and cross-group transaction boundaries

Exit condition: membership can change safely and scale claims are backed by
multi-group benchmarks rather than a single serialized log.
