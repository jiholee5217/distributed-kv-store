# Client and failure semantics

The API provides linearizable successful operations, not exactly-once request
execution. Clients must distinguish a committed response from an unavailable or
unknown outcome before retrying.

## Response meanings

| Client observation | Meaning | Safe next action |
| --- | --- | --- |
| `201 Created` from `PUT` | The leader persisted the command on a majority and applied it locally | Treat the value as committed |
| `200 OK` from `GET` | A no-op barrier committed in the leader's term before the value was read | Treat the value as linearizable at the returned read index |
| `204 No Content` from `DELETE` | The deletion committed and applied on the leader | Treat the key as deleted |
| `404 Not Found` from `GET` | The read barrier committed, then the key was absent | Treat absence as linearizable |
| `503 Service Unavailable` before dispatch | No known leader or no majority was available | Retry with bounded exponential backoff and a deadline |
| Connection loss or client timeout | The request may have failed before admission or after commit | Outcome is unknown; verify state before retrying when duplicates matter |

Followers forward a request at most once and add `X-Raft-Forwarded` to prevent a
loop. During an election, followers return 503 until they learn the new leader.

## Why retries are not exactly once

A leader can commit a write and lose the connection before its response reaches
the client. The client cannot distinguish that from a failure before commit.
Retrying the same `PUT` value is idempotent at the materialized key-value state,
but it can append another log entry. There is no durable request-ID table or
deduplicated result cache.

A production client would use:

- a total operation deadline;
- jittered exponential backoff for 503 responses;
- bounded retry attempts;
- application-level request IDs when duplicate side effects matter; and
- a verification read after an unknown write outcome.

## Read safety

An isolated old leader may not immediately know that a higher-term leader exists.
Serving its local map directly could return stale data. This implementation first
proposes and commits a barrier entry for every `GET`. The barrier succeeds only if
the node still leads a majority in its current term.

This is simple and safe, but it means reads grow the log and pay majority
persistence latency. An optimized Raft `ReadIndex` protocol remains roadmap work.

## Health versus readiness

`/healthz` reports process liveness and Raft identity. A 200 response does not
promise that the node currently knows a leader or can reach a majority. Use
`/v1/status`, the role/term metrics, and a real operation when checking consensus
readiness.
