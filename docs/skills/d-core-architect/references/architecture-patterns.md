# D-Core Architecture Patterns

Use this reference when you need concrete examples from the repo while designing a new abstraction.

## Core shape

The common d-core layering is:

1. Stable protocol
2. One or more components implementing that protocol
3. Thin clients that talk to the external system
4. Integrant wiring through `ig/init-key`, `ig/halt-key!`, and optional Duct modules

## Protocol examples

- `src/d_core/core/cache/protocol.clj`
  `CacheProtocol` exposes portable operations like lookup, put, delete, and clear.
- `src/d_core/core/producers/protocol.clj`
  `Producer` exposes `produce!` with a portable message map and backend-specific `options`.

The pattern is consistent:

- protocol methods operate on portable data
- backend-specific knobs go in `opts`/`options`
- consuming code should not need vendor objects

## Component examples

### Backend component implementing a protocol

- `src/d_core/core/cache/redis.clj`
  `RedisCache` implements `CacheProtocol` and depends on `redis-client`.

This file shows the expected split:

- helper fns wrap low-level operations
- the record implements the stable protocol
- `ig/init-key` assembles the record from explicit dependencies

### Shared orchestration component

- `src/d_core/core/producers/common.clj`
  `CommonProducer` implements `Producer` and coordinates routing, tracing, schema validation, deferred delivery, and delegation to concrete producers.

Use a common component like this only when there is real shared cross-backend behavior. Do not introduce one just to forward calls.

## Client examples

### Thin Integrant wrapper

- `src/d_core/core/clients/redis.clj`
- `src/d_core/core/clients/postgres.clj`

These namespaces usually:

- expose an Integrant key
- call `impl/make-client`
- optionally call `impl/close!` on halt

### Low-level implementation namespace

- `src/d_core/core/clients/redis/client.clj`
- `src/d_core/core/clients/postgres/client.clj`

These namespaces hold vendor-facing setup and teardown. They should not absorb domain orchestration or protocol-level behavior.

## Module example

- `src/d_core/module/graphql.clj`

This shows when a Duct module is appropriate:

- one higher-level config entry expands into multiple concrete Integrant keys
- dependencies remain explicit through `ig/ref`
- the module reduces repetitive wiring without hiding the graph

## Naming conventions

- Protocol namespaces: `src/d_core/core/<domain>/protocol.clj`
- Backend implementation namespaces: `src/d_core/core/<domain>/<backend>.clj`
- Shared orchestration namespaces: `src/d_core/core/<domain>/common.clj`
- Client wrapper namespaces: `src/d_core/core/clients/<backend>.clj`
- Low-level client implementation namespaces: `src/d_core/core/clients/<backend>/client.clj`
- Integrant keys usually mirror the namespace and responsibility

## Design checklist

Before adding a new abstraction, check:

- Is this reusable infrastructure rather than business logic?
- Is a stable protocol justified?
- Can the protocol stay portable?
- Does the component depend explicitly on clients and collaborators?
- Does config choose the backend rather than code branches?
- Is resource lifecycle obvious?
- Could this be a pure function instead of a new Integrant key?
