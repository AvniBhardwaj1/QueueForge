# QueueForge architecture

These diagrams describe the code as it is implemented. `failed` is a supported database/API status that can be manually retried, but the current worker sends failures straight back to `queued` or marks them `dead`.

## Components

```mermaid
flowchart LR
    Client -->|HTTP requests| API[API handlers]
    API -->|check client quota| Limit[Rate limiter]
    API -->|enqueue, query, retry, cancel| Store[Store interface]
    Store -->|SQL operations| SQLite[(SQLite WAL)]
    Dispatcher -->|atomic claim| Store
    Dispatcher -->|send job| Pool[Worker pool]
    Pool -->|invoke registered type| Handlers[Job handlers]
    Handlers -->|result or error| Pool
    Pool -->|save outcome and heartbeat| Store
    Reaper -->|re-queue stale locks| Store
    Dashboard -.->|would call REST API| API
```

## Job submission and execution

```mermaid
sequenceDiagram
    actor Client
    participant API as HTTP API
    participant DB as SQLite store
    participant Dispatcher
    participant Worker
    participant Handler as Registered handler

    Client->>API: POST /jobs with job JSON
    API->>DB: Insert queued job
    DB-->>API: Return stored job
    API-->>Client: 201 Created with job
    loop Every poll interval
        Dispatcher->>DB: Claim next eligible job
        DB-->>Dispatcher: Atomically mark and return running job
    end
    Dispatcher->>Worker: Send claimed job
    Worker->>Handler: Run with timeout context
    Handler-->>Worker: Result
    Worker->>DB: Save result and mark succeeded
    Client->>API: GET /jobs/{id}
    API->>DB: Read job status
    DB-->>API: Job details
    API-->>Client: 200 OK with succeeded job
```

## Job state machine

```mermaid
stateDiagram-v2
    [*] --> queued: API creates job
    queued --> running: dispatcher claims eligible job
    queued --> cancelled: client cancels
    running --> succeeded: handler returns successfully
    running --> queued: handler fails and attempts remain
    running --> dead: handler fails on final attempt
    running --> queued: reaper recovers stale lock
    failed --> queued: client retries supported failed status
    dead --> queued: client retries and resets attempts
    succeeded --> [*]
    cancelled --> [*]
```

`failed` is reserved for compatibility with the retry endpoint; there is no current code path that sets a job to `failed`. Handler failures go directly to `queued` (retry) or `dead` (attempts exhausted).
