# ADR 0007: Pluggable Temporal Backend for Workers System

Date: 2026-07-10
Status: Proposed

## Context

The current workers system (`d-core.libs.workers`) handles async jobs through two hardcoded worker kinds (`:ticker` and `:command`), heavily relying on `core.async` channels. We want to introduce Temporal to the system as a distributed, pull-based backend for long-running workflows and activities. 

To support this without altering the core `core.async` logic or introducing backwards-incompatible changes, we need to adapt the worker system to support pluggable backends. Currently, the `start-workers` function utilizes a hardcoded `case` statement to bootstrap the known worker kinds.

## Decision Drivers

- We need to run Temporal workers seamlessly alongside existing `core.async` workers.
- The worker topology and configuration must remain declarative and app-owned.
- The Temporal Java SDK boilerplate must be abstracted behind our declarative configuration.
- We must avoid breaking changes to existing `:command` and `:ticker` workers.
- The worker framework should be extensible for other potential backends in the future.

## Considered Options

### 1) Separate Temporal Bootstrapping
- **Pros**: Leaves `d-core.libs.workers` completely untouched.
- **Cons**: Temporal lifecycle is totally separate from the existing `start-workers` and `stop!` behavior, leading to scattered initializations, more boilerplate for developers, and disjointed integrant lifecycle management.

### 2) Pluggable Worker Kinds via `defmulti` (Selected)
- **Pros**: Extends the existing `start-workers` system to recognize new worker `:kind`s dynamically. We can build a new `:temporal` worker plugin that handles Temporal's Java WorkerFactory, bridging activities into the core.async world using existing features like `request!`.
- **Cons**: Requires refactoring `start-workers` to use `defmulti`.

## Decision

We will refactor the worker initialization in `d-core.libs.workers` to dispatch via a `defmulti` function (e.g., `start-worker-by-kind`) keyed on the `:kind` of the worker.

- Existing `:ticker` and `:command` logic will be migrated into their respective `defmethod` definitions.
- We will add a new namespace (`d-core.libs.workers.temporal`) that implements `defmethod start-worker-by-kind :temporal`.
- The `:temporal` plugin will manage the Temporal Java SDK Worker lifecycle (retrieving the Temporal Client from components, starting the `WorkerFactory`, registering activities, and attaching the shutdown sequence to the worker system's `stop-chan`).
- Temporal Activities will serve as bridges. When Temporal pulls a task, the activity implementation can use `workers/request!` to submit the work to existing `:command` workers, wait on the `core.async` reply channel, and return the result back to Temporal.

## Consequences

- The declarative configuration for workers becomes fully extensible to new backends.
- The new dependency on the Temporal Java SDK is isolated to a specific namespace, meaning projects that don't need Temporal aren't forced to load its classes.
- Developers can add `:temporal` workers seamlessly and tie them into their existing `core.async` pipelines using the same Integrant lifecycle.
- Backward compatibility is strictly maintained for existing `core.async` implementations.

## Next Steps

- Implement the `defmulti` refactoring in `d-core.libs.workers`.
- Build the `d-core.libs.workers.temporal` namespace.
- Add proxy utilities to translate Temporal's Java Activity interfaces to the configured Clojure functions.
