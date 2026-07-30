# Architecture and consistency model

## Request path

Any node can act as a coordinator:

1. Rendezvous hashing ranks the cluster members for the request key.
2. The first `N` members become the replica set.
3. The coordinator contacts all selected replicas concurrently.
4. A write returns success after at least `W` acknowledgements.
5. A read requires at least `R` valid responses and returns the record with the
   greatest version.
6. After a successful read, stale responding replicas receive the newest record
   in the background.

The current implementation waits for every request to finish or time out before
responding, even after reaching quorum. Early quorum responses and cancellation
are an intentional future optimization.

## Key placement

For every `(key, member ID)` pair, the node computes a SHA-256 score. The
members with the highest scores own the key. This is rendezvous hashing, also
called highest-random-weight hashing.

Its useful properties here are:

- every coordinator independently computes the same replica set;
- a membership change remaps only keys affected by that member; and
- no virtual-node tuning is required for the first implementation.

This prototype uses static membership supplied at process startup. It does not
yet move existing data when membership changes.

## Versions and conflicts

A coordinator versions a write with `(wall time in nanoseconds, node ID)`.
Each node advances a monotonic local clock when it generates or observes a
version. The node ID deterministically breaks equal-timestamp ties.

This produces convergent last-write-wins behavior but has an explicit
limitation: physical clock skew can make a causally older write appear newer.
Hybrid logical clocks, vector clocks, or consensus are possible follow-up
designs depending on the desired consistency and conflict semantics.

## Deletes

Deletes are replicated as versioned tombstones rather than physically removing
the key. A delayed older value therefore cannot overwrite a newer deletion.
Tombstones are not garbage-collected yet; safe compaction requires evidence
that every replica has observed the delete.

## Failure model

With the default `N=3, R=2, W=2`, one replica may be unavailable while reads and
writes still reach quorum. The service returns `503 Service Unavailable` when
it cannot reach the configured quorum.

The implementation handles crash-stop node failures and network unavailability
at the request layer. It does not yet address Byzantine behavior, authenticated
replica traffic, dynamic failure detection, or data persistence.

## Consistency boundary

When `R + W > N`, successful read and write quorums overlap. However,
last-write-wins versions are generated without consensus, and concurrent
coordinators can race. The system is eventually convergent; it should not be
described as linearizable.
