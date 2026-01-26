# ADR 0002: Workers System (core.async)

Date: 2026-01-25
Status: Proposed

## Context

D-core already provides messaging, scheduling, and other infrastructure
components, while keeping service-specific wiring (routes, handlers, workers)
app-owned. We now want a lightweight workers system that is built on
`clojure.core.async` and supports common async patterns without introducing a
new framework.

Core.async has strict rules about where blocking is allowed. Blocking calls
inside a `go`/`go-loop` can starve the scheduler pool and has caused production
incidents. We want to reduce these foot-guns by making the execution model
explicit and safe by default.

Two recurring patterns are driving this:

1) Internal repeating jobs. Example: a clock worker emits a tick on a channel
at a fixed interval, triggering a pipeline.
2) An async producer/consumer or command channel. Example: a feature is
triggered by putting a specific input into an exposed channel (from HTTP
handlers, consumers, or other app components).

The main design challenge is how to make D-core components (the same ones
available to HTTP handlers and consumer handlers) available to a core.async
pipeline of functions and channels, without coupling application logic to the
runtime implementation.

## Decision Drivers

- Use `core.async` as the base building block.
- Provide common worker patterns with minimal abstraction.
- Integrate with Integrant lifecycle (start/stop, config-first wiring).
- Make component injection explicit and uniform for all handlers.
- Keep app-specific logic and wiring in the application.
- Provide clear backpressure semantics and simple observability hooks.
- Make blocking vs non-blocking execution explicit to avoid go-loop misuse.

## Considered Options

### 1) App-only core.async loops (no D-core support)
- Pros: Maximum flexibility, no new API.
- Cons: Repeated boilerplate; no standard for component injection or lifecycle
  management; inconsistent patterns across services.

### 2) External worker framework (non-core.async)
- Pros: Rich features (scheduling, retries, supervision).
- Cons: New dependency and mental model; diverges from core.async usage;
  integration with Integrant and D-core components is less direct.

### 3) D-core workers runtime on core.async with explicit execution modes (selected)
- Pros: Reuses core.async; standardizes common patterns; integrates cleanly with
  Integrant; keeps worker logic app-owned.
- Cons: Requires some new configuration and a small runtime implementation.

## Decision

Add a small D-core workers system built on `core.async`. It provides:

- A registry of named channels with configurable buffers.
- Worker definitions for common patterns:
  - `:ticker` (repeating trigger)
  - `:command` (exposed input channel)
- Worker entrypoints are functions provided by the app (no keyword handler
  resolution; messaging handlers remain separate).
- Context injection: each worker function receives a context map that includes the
  components needed by the pipeline (e.g., db, cache, producers), along with
  worker metadata and channels.
- Integrant lifecycle: starting a workers system spins up go-loops; halting it
  closes channels and stops loops.
- Execution model per worker (`:dispatch`), so blocking handlers can run on a
  thread pool separate from go blocks.
- Worker topology is app-owned: D-core defines entry points and workload
  requirements, while fan-out/fan-in is composed by the developer.
- Observability is provided via the existing Duct observability component,
  available in the worker context.
- Supervision/restarts are out of scope for v1; failures are app-defined and
  may leverage async messaging + DLQ when needed.
- The runtime should provide a dev-mode guard that detects blocking calls in
  `:dispatch :go` workers and fails fast in development/testing, while avoiding
  additional risk in production.

The context map is the primary solution for component injection. The runtime
builds it once per worker and passes it to every handler, so worker handlers can
use the same components that HTTP handlers and consumer handlers use.

Pipelines remain app-owned: developers compose channels and handlers directly
in application code, using the exposed channels and the injected context. D-core
does not impose a pipeline DSL in v1.

## Execution Model (Blocking Safety)

Worker functions must declare how they run:

- `:dispatch :go` for non-blocking handlers (core.async ops only).
- `:dispatch :thread` for handlers that may block (I/O, DB, sleeps, CPU-heavy).

` :thread` uses core.async blocking facilities (e.g., `async/thread` or
`pipeline-blocking`) which run on a separate pool from go blocks. This makes
the default failure mode explicit and reduces accidental blocking inside
go-loops.

## Consequences

- Standard patterns reduce boilerplate for repeaters and command channels.
- App code remains the owner of worker logic and wiring.
- Requires careful defaults for buffer sizes and blocking behavior.
- We need to document go-block vs thread-block decisions to avoid deadlocks.
- Observability should be added consistently (logging/metrics/tracing in ctx).
- Mis-declared handlers (blocking on `:go`) can still cause problems; guardrails
  and clear docs are required.
- Since supervision is out of scope, failures require explicit app-level
  handling or integration with messaging/DLQ.

## Config Sketch

Illustrative config shape (exact keys TBD):

```edn
{:my-app.config.workers/definition
 {:channels {:clock/ticks {:buffer 1}
             :commands/in {:buffer 32}}
  :workers
  {:clock {:kind :ticker
           :interval-ms 1000
           :out :clock/ticks
           :dispatch :go}
   :commands {:kind :command
              :in :commands/in
              :worker-fn #ig/ref :my-app.workers/commands
              :dispatch :thread
              :expose? true}}}

 :d-core.core.workers/system
 {:definition #ig/ref :my-app.config.workers/definition
  :components {:db #ig/ref :d-core.core.db/main
               :cache #ig/ref :d-core.core.cache/main
               :producers #ig/ref :d-core.core.producers/registry}}}
```

Worker function signature (example):

```clojure
(defn process-order [{:keys [components channels worker]} msg]
  (let [{:keys [db cache producers]} components]
    ;; business logic here
    ))
```

Command-style workers can support request/reply by allowing messages to carry a
`reply-chan` (e.g., a `promise-chan`) that the handler can put a response on.

Pipeline composition stays in app code. Example (sketch):

```clojure
(async/pipeline-blocking 16 out in
  (map (fn [msg] (process-order ctx msg))))
```

## Scope (v1)

- Ticker workers based on `core.async/timeout`.
- Command workers with exposed input channels.
- Context injection of Integrant components and worker metadata.
- Clean start/stop lifecycle with channel closure on halt.
- Explicit execution mode (`:dispatch`) per worker.

## Non-goals

- Distributed or durable job scheduling (use messaging/queues for that).
- Automatic retries, backoff, or exactly-once guarantees.
- A full workflow DSL for complex graphs (keep v1 simple).

## Next Steps

- Define the worker config schema and handler signatures.
- Implement a minimal runtime in `d-core.core.workers`.
- Add dev examples and tests (ticker + pipeline + command).
- Document usage and integration with HTTP handlers and consumers.

## Open Questions

- None (v1 scope defined).
