# Diagram gallery

These diagrams describe the implemented five-node system. Planned storage and
scaling work is isolated in the [roadmap](roadmap.md).

## 1. Runtime topology

```mermaid
flowchart TB
    C["HTTP client"] -->|"request to any port"| N1

    subgraph D["Docker network: one static Raft group"]
        N1["node-1"]
        N2["node-2"]
        N3["node-3"]
        N4["node-4"]
        N5["node-5"]
    end

    N1 <-->|"RequestVote and AppendEntries"| N2
    N1 <-->|"peer RPCs"| N3
    N1 <-->|"peer RPCs"| N4
    N1 <-->|"peer RPCs"| N5

    N1 --> V1["volume 1"]
    N2 --> V2["volume 2"]
    N3 --> V3["volume 3"]
    N4 --> V4["volume 4"]
    N5 --> V5["volume 5"]

    N1 --> P["Prometheus"]
    N2 --> P
    N3 --> P
    N4 --> P
    N5 --> P
    P --> G["Grafana"]
```

## 2. Majority-backed write

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant F as Any follower
    participant L as Leader
    participant A as Follower A
    participant B as Follower B
    participant S as KV state machine

    C->>F: PUT /v1/kv/key
    F->>L: Forward once
    L->>L: Append and persist entry
    par Replicate missing suffix
        L->>A: AppendEntries
        L->>B: AppendEntries
    end
    A->>A: Persist log
    B->>B: Persist log
    A-->>L: Success and match index
    B-->>L: Success and match index
    L->>L: Three copies; advance commit index
    L->>S: Apply command in order
    L-->>C: 201 Created
```

The leader plus any two followers form the majority of three. Acknowledging
before that point would allow a later leader to lose a supposedly successful
write.

## 3. Leader failure and replacement

```mermaid
sequenceDiagram
    participant L1 as Leader, term T
    participant F1 as Follower A
    participant F2 as Follower B
    participant C as Clients

    L1->>F1: Heartbeats
    L1->>F2: Heartbeats
    L1--xL1: Crash or network loss
    C->>F1: Request
    F1-->>C: 503 while no leader is known
    Note over F1,F2: Randomized election deadline expires
    F1->>F1: Become candidate, term T+1, vote for self
    F1->>F2: RequestVote with last log index and term
    F2-->>F1: Vote granted
    Note over F1: Majority vote reached
    F1->>F1: Become leader and append term barrier
    F1->>F2: AppendEntries
    C->>F1: Retry request
    F1-->>C: Committed response
```

## 4. Safety versus availability

```mermaid
flowchart LR
    A["Mutually reachable nodes"] --> Q{"At least 3 of 5?"}
    Q -->|"Yes"| E["Can elect a leader"]
    E --> W["Writes and read barriers can commit"]
    Q -->|"No"| M["Minority partition"]
    M --> R["Reject or time out requests"]
    R --> S["Preserve one committed history"]
```

The system chooses consistency over availability when no majority exists.

## 5. Conflicting-log recovery

```mermaid
flowchart LR
    L["Current leader suffix"] --> P["AppendEntries includes previous index and term"]
    P --> C{"Follower prefix matches?"}
    C -->|"Yes"| A["Append missing entries"]
    C -->|"No"| B["Return conflict index"]
    B --> D["Leader backs up next index"]
    D --> P
    A --> X["Follower truncates conflicting suffix"]
    X --> Y["Logs converge on leader history"]
```

## 6. Persistence and restart

```mermaid
flowchart TD
    M["Term, vote, log, commit index"] --> J["Encode complete JSON state"]
    J --> H["Wrap with version and SHA-256 checksum"]
    H --> T["Write temporary file"]
    T --> F["fsync temporary file"]
    F --> R["Atomic rename over prior state"]
    R --> D["fsync parent directory"]
    D --> C["Process crash and restart"]
    C --> Q["Verify version and checksum"]
    Q --> V["Validate sentinel, indexes, and commit bound"]
    V --> A["Replay entries 1 through commit index"]
    A --> K["Reconstructed KV state machine"]
    K --> P["Leader reconciles any uncommitted suffix"]
```

The transparent full-state format is easy to study but becomes increasingly
expensive as the log grows. WAL segmentation, record-level checksums,
crash-point tests, and snapshots remain future storage milestones.
