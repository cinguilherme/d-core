## D-Core

### Introduction

D-core is a **Duct/Integrant battery pack**: a collection of reusable components, defaults, and protocols intended to make Duct applications mostly about **business logic + configuration**.

It is intentionally **coupled to Integrant and Duct**. The “API surface” is primarily **Integrant init-keys** and a small set of supporting namespaces/protocols.

## Major Rules of D-Core Abstractions

 - Every component needs to adher to a common protocol for the given abstraction (e.g. `CacheProtocol`, `StorageProtocol`, `MessagingProtocol`, etc.) this can be interpreted as a Facade pattern, but the Protocols have 2 things that are important:
 -- They are generic in the sense that they don't recieve even specific objects, always a clojure primitive, or a map of primitives, or a list of primitives, etc.
 -- They take a opts last arument to allow for speficic that are specific to a particular implementation, such as in MessagingProtocol, the last argument is where you can find a Kafka specificitites, or a Redis specificities, etc.

### What it provides

- **Integrant components** for common app infrastructure:
  - messaging (routing, producers/consumers, codecs, dead-letter)
  - cache (in-memory + redis-backed)
  - storage (local-disk + minio/s3-style)
  - clients (redis, kafka, jetstream/nats, sqlite/postgres, datomic (Work in Progress), typesense)
  - http client (policy wrapper: rate-limit, bulkhead, circuit breaker, retries)
  - graphql server (Lacinia + optional GraphiQL + subscriptions)
  - metrics (Prometheus registry + scrape server)
  - rate limiting (sliding window, leaky bucket)
  - cron tasks (Quartz-backed scheduler)
  - tracing helpers + Ring middleware
  - simple in-memory queues for local/dev and testing
- **Swappable implementations** behind stable protocols (so apps can keep the same business logic across envs).
- **Sane defaults** designed for local development while allowing Duct apps to override everything via config.

### What it does NOT provide

- **Business logic**: domain behavior belongs in the consuming service.
- **Service-specific wiring**: routes, handlers, workers, and app-level “what subscribes to what” decisions live in the app.

### Design principles

- **Configuration-first**: apps wire behavior via `duct.edn` and Integrant refs.
- **Replaceable backends**: choose infra via config, not code changes.
- **Incremental adoption**: you can migrate a service piece-by-piece (queue/tracing → messaging → clients → producers/consumers → cache/storage/etc).