# ADR 0001: Caching Layer Models

Date: 2026-01-24
Status: Accepted

## Context

D-core's current layered cache is optimized for key/value access patterns and is
highly effective for those use cases. We now want to support caching in front
of stateful databases (e.g., Postgres) where cache consistency, invalidation,
and update propagation introduce substantially different constraints and
failure modes.

In these scenarios, cache behavior is not a purely local implementation detail.
The caching model is an architectural decision that affects correctness,
latency, and operational complexity.

## Decision Drivers

- Correctness expectations (staleness tolerance, read-your-writes, monotonic
  reads).
- Invalidation strategy and source of truth.
- Operational complexity and failure handling.
- Performance profile (read heavy vs write heavy).
- Integration with existing D-core infrastructure (queues, messaging, cron).

## Considered Options

### 1) Cache-aside (read-through with explicit invalidation)
- Pros: Simple mental model, minimal write-path coupling.
- Cons: Cache stampede risk, explicit invalidation required, eventual
  consistency on updates.

### 2) Write-through
- Pros: Stronger consistency on reads, simpler invalidation (cache is updated on
  writes).
- Cons: Higher write latency, cache availability affects writes, write
  amplification.

### 3) Write-behind (async)
- Pros: High write throughput, decoupled writes, good for high-read systems with
  acceptable staleness.
- Cons: Risk of data loss on failures, eventual consistency, reordering issues.
  - data loss mitigation via event sourcing (e.g. Kafka, Redis Streams, etc.) we have support for this in the messaging layer

### 4) CDC-driven cache (event-driven/materialized cache)
- Pros: Strong ordering, consistency aligned with DB commit log, clear audit
  trail.
- Cons: Requires CDC infrastructure, more operational complexity, larger blast
  radius on failure.

#### CDC infrastructure requirements (baseline)

- Database configuration to emit change events (e.g., logical replication /
  WAL decoding).
- A capture pipeline (e.g., Debezium or native logical replication) that can
  publish ordered change events.
- A durable transport (Kafka/NATS/JetStream/etc) with replay and consumer
  offsets to enable rebuilds.
- Schema management for event payloads (compatibility, versioning).
- Idempotent cache writers and ordering guarantees per key / entity.
- Backfill / snapshot strategy for initial cache population.
- Monitoring for lag, dropped events, and consumer health.

#### Operational risks

- Lag accumulation can create prolonged staleness or downstream load spikes.
- Backpressure or broker outages can stall cache convergence.
- Replays can overwhelm cache writers without rate controls.
- Out-of-order or duplicated events can corrupt cache without idempotency.

## Decision

Support multiple cache models in D-core, exposed through configuration and
stable protocols. The cache layer will define a clear contract for consistency
expectations and invalidation semantics, while implementations can leverage
existing D-core infrastructure (queues, messaging, cron) to realize the chosen
model.

## Consequences

- We must define cache contracts that are explicit about staleness and ordering.
- Different cache models will have different failure modes; observability must
  surface those (lag, staleness, error rates).
- Implementations will likely need distinct operational playbooks.

## Config Sketch

Illustrative config shape (exact keys TBD):

```edn
{:d-core/cache
 {:model :cache-aside
  :consistency {:staleness-ms 5000
                :read-your-writes? false}
  :invalidation {:mode :ttl
                 :ttl-ms 60000
                 :explicit? false}
  :write-behind {:queue :messaging/cache-writes
                 :max-lag-ms 10000}
  :cdc {:source :postgres-sqs
        :consumer :tbd}}}
```

## Scope (v1)

- Support cache model selection via configuration.
- CDC scope: Postgres SQS only, with a Postgres consumer strategy to be defined.
- Invalidation defaults: cache-aside prefers TTL; other models prefer explicit
  invalidation with optional TTL backstop.
- Testing: validate CDC streaming, out-of-order updates, and recovery using the
  docker-compose environment.

## Non-goals

- Query-result caching (joins/sorts/pagination) as a first-class feature.
- Automatic CDC provisioning or schema management beyond minimal requirements.
- Strong global consistency guarantees across regions/environments.

## Next Steps

- Define the cache protocol contract for consistency and invalidation semantics.
- Add a Postgres CDC consumer strategy and integration path in D-core.
- Extend docker-compose tests for CDC streaming, ordering, and recovery.

## Open Questions

- Invalidation mode should be configurable per model; define defaults
  (cache-aside favors TTL, others favor explicit with optional TTL backstop).
- How to express consistency expectations in config (per-cache or per-key
  class).
- CDC scope for v1: Postgres SQS. Open: consumer strategy for a Postgres source
  inside D-core.
- Testing strategy: extend docker-compose coverage to validate CDC streaming,
  out-of-order updates, and recovery scenarios.
