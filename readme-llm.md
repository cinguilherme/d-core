# D-Core LLM Guide

This file is a compact guide for LLMs and developers integrating D-Core into
new Duct/Integrant codebases. It focuses on how to map app features to D-Core
components and how to wire them safely.

## What D-Core is (and is not)

- D-Core is a Duct/Integrant battery pack: infrastructure components wired by
  config, not business logic.
- The public surface is Integrant keys plus a set of protocols.
- Apps own routes, handlers, and domain decisions; D-Core owns infra.

## Rules of engagement for LLMs

- Use configuration-first wiring in `duct.edn` with `#ig/ref`.
- Keep handlers in the app; only pass references from config.
- Prefer swapping backends via config, not new code paths.
- Protocols take primitive data and accept `opts` as the last argument.
- When unsure, read `docs/` or the component namespace under `src/d_core/`.

## Capability map (diagram -> D-Core component)

Use this to translate drawings into concrete D-Core wiring:

- HTTP ingress -> your Ring routes + `:d-core.core.tracing.http/middleware`.
- GraphQL API -> `:d-core.module/graphql` (see `docs/graphql.md`).
- Async workflows / queues -> `:d-core.core.messaging/routing` plus
  `:d-core.core.producers.*` and `:d-core.core.consumers.*`
  (see `docs/async_messaging.md`).
- Dead letters -> `:d-core.core.messaging.dead-letter/*`
  (see `docs/dead_letters.md`).
- Cache -> `:d-core.core.cache.common/common` plus a backend
  (`:d-core.core.cache.redis/redis`, `:d-core.core.cache.in-memory/in-memory`,
   `:d-core.core.cache.local-file/local-file`).
- Object storage -> `:d-core.core.storage/common` plus backend
  (`:d-core.core.storage/minio`, `:d-core.core.storage/local-disk`).
- SQL DB -> `:d-core.core.databases.sql/common` plus backend
  (`:d-core.core.databases.postgres/db`, `:d-core.core.databases.sqlite/db`).
- Datomic -> `:d-core.core.databases.datomic/db`.
- HTTP client -> `:d-core.core.http/client` or `:d-core.core.http/clients`.
- Search -> `:d-core.core.text-search/common` with
  `:d-core.core.text-search.typesense/engine`.
- Geo -> `:d-core.core.geo.tile38/index`.
- Rate limiting -> `:d-core.core.rate-limit.sliding-window/limiter` or
  `:d-core.core.rate-limit.leaky-bucket/limiter`.
- Cron jobs -> `:d-core.libs.cron-task/scheduler` (see `docs/cron_task.md`).
- Metrics -> `:d-core.core.metrics.prometheus/*`.
- Dev/test queues -> `:d-core.queue/in-memory-queue` or
  `:d-core.queue/in-memory-queues`.

## Wiring patterns (safe, minimal examples)

Messaging wiring requires a codec plus routing, then producers and runtimes.
Keep the routing map app-owned and inject handlers with `#ig/ref`.

```edn
{:system
 {:d-core.core.messaging.codecs/json {}
  :d-core.core.messaging/codec {:codec #ig/ref :d-core.core.messaging.codecs/json}

  :d-core.core.messaging/routing
  {:topics {:orders {}}
   :publish {:orders {:targets [{:producer :redis-primary :stream "core:orders"}]}}
   :subscriptions {:orders-worker {:topic :orders
                                   :source :redis
                                   :client :redis-primary
                                   :handler #ig/ref :my-app.handlers/orders}}}

  :d-core.core.clients.redis/client {:uri "redis://localhost:6379"}

  :d-core.core.producers.redis/producer
  {:redis #ig/ref :d-core.core.clients.redis/client
   :routing #ig/ref :d-core.core.messaging/routing
   :codec #ig/ref :d-core.core.messaging/codec}

  :d-core.core.producers.common/producer
  {:default-producer :redis-primary
   :routing #ig/ref :d-core.core.messaging/routing
   :producers {:redis-primary #ig/ref :d-core.core.producers.redis/producer}}

  :d-core.core.consumers.redis/runtime
  {:redis {:redis-primary #ig/ref :d-core.core.clients.redis/client}
   :routing #ig/ref :d-core.core.messaging/routing
   :codec #ig/ref :d-core.core.messaging/codec}}}
```

HTTP clients are config-only and easy to swap per environment:

```edn
{:system
 {:d-core.core.http/clients
  {:payments {:base-url "https://api.example.com"
              :default-headers {"Accept" "application/json"}
              :policies {:rate-limit {:rate-per-sec 50 :burst 100}
                         :bulkhead {:max-concurrent 20}
                         :circuit-breaker {:failure-threshold 5}
                         :retry {:max-attempts 3}}}}}}
```

Cron tasks are dispatch-only; business logic stays in your handlers:

```edn
{:system
 {:d-core.libs.cron-task/scheduler
  {:handlers {:cleanup #ig/ref :my-app.handlers/cleanup}
   :tasks {:cleanup {:cron "0 0 * * * ?"
                     :handler :cleanup
                     :payload {:limit 100}}}
   :start? true}}}
```

## Protocols and facades (stable code targets)

When you need to call D-Core directly (e.g., from handlers), prefer these
protocols:

- Cache: `d-core.core.cache.protocol/CacheProtocol`
- Storage: `d-core.core.storage.protocol/StorageProtocol`
- Rate limit: `d-core.core.rate-limit.protocol/RateLimitProtocol`
- Metrics: `d-core.core.metrics.protocol/MetricsProtocol`
- Text search: `d-core.core.text-search.protocol/*`
- SQL: `d-core.core.databases.protocols.simple-sql/*`
- Datomic: `d-core.core.databases.protocols.datomic/DatomicProtocol`
- Geo: `d-core.core.geo.protocol/GeoIndexProtocol`
- Dead letters: `d-core.core.messaging.dead-letter.protocol/DeadLetterProtocol`

## Drawing-to-implementation checklist

1. Identify sync entrypoints (HTTP/GraphQL) vs async flows (messaging).
2. Choose storage/caching/search components for each data store in the diagram.
3. Create the routing map for topics, publish targets, and subscriptions.
4. Wire Integrant keys in `duct.edn` using `#ig/ref`.
5. Implement handlers in the app; keep D-Core as infra only.
6. Add metrics/tracing if the diagram includes observability.

## References in this repo

- `readme.md` (overview and examples)
- `docs/async_messaging.md`
- `docs/dead_letters.md`
- `docs/cron_task.md`
- `docs/graphql.md`
- `docs/time.md`
- `docs/supported.md`
