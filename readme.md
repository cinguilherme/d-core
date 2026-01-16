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
  - clients (redis, kafka, jetstream/nats, sqlite/postgres, datomic, typesense)
  - http client (policy wrapper: rate-limit, bulkhead, circuit breaker, retries)
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

### Usage

#### 1) Add dependency

For local development in a monorepo:

```clojure
{:deps {d-core/d-core {:local/root "../d-core"}}}
```

For a released version (once published to Clojars):

```clojure
{:deps {your-group/d-core {:mvn/version "x.y.z"}}}
```

#### 2) Wire Integrant keys in `duct.edn`

At a high level, your app typically provides:
- handler fns (business logic)
- routing/subscription map (what topics exist, what handler processes each subscription)

And `d-core` provides the infrastructure keys:
- `:d-core.core.messaging/*`
- `:d-core.core.clients/*`
- `:d-core.core.producers/*`
- `:d-core.core.consumers/*`
- `:d-core.core.cache/*`
- `:d-core.core.storage/*`
- `:d-core.core.tracing.http/middleware`
- `:d-core.queue/*`

Example (illustrative):

```edn
{:system
 {:duct.module/web {:middleware [:d-core.core.tracing.http/middleware]}

  ;; app-level routing (handlers are refs to your app namespaces)
  :my-app.config.messaging/routing
  {:handlers {:order-created #ig/ref :my-app.handlers/order-created}
   :topics {:orders {:source :redis}}
   :subscriptions {:orders-sub {:topic :orders :handler :order-created :source :redis}}}

  ;; d-core routing facade points at your app routing
  :d-core.core.messaging/routing #ig/ref :my-app.config.messaging/routing

  ;; rest of d-core keys: clients, producers, consumers, cache, storage...
  }}
```

HTTP client example (illustrative):

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

### Repo layout expectation

- D-Core code lives under `src/d_core/**` and uses the `d-core.*` namespace prefix.

### Supported vs unsupported matrix

See `docs/supported.md`.

### Dead letters

See `docs/dead_letters.md`.
