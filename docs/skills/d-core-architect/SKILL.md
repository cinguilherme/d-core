---
name: d-core-architect
description: Design new d-core abstractions and backend architecture that fit this repository's Duct and Integrant patterns. Use when Codex must propose or refine protocols, components, clients, modules, config wiring, or subsystem boundaries in d-core, especially when the solution should follow the stable protocol to component to client layering used across the codebase.
---

# D-Core Architect

## Overview

Design additions and refactors that match d-core's existing abstractions: stable protocols, Integrant-wired components, thin clients at the edge, and configuration-first wiring through Duct.

Use this skill to produce architecture proposals that fit the repo. If a request belongs in service-level business logic rather than d-core infrastructure, say so clearly and keep it out of the abstraction design.

Read [references/architecture-patterns.md](references/architecture-patterns.md) when you need concrete repo examples or naming patterns.

## Design Workflow

1. Identify the boundary.
   Decide whether the problem is infrastructure, cross-backend integration, or service-specific business logic.
2. Choose the minimum abstraction.
   Add a new protocol only if there is a stable capability worth preserving across implementations.
3. Place each responsibility in the right layer.
   Keep vendor APIs in clients, reusable backend behavior in components, and only portable operations in protocols.
4. Wire with config, not code branching.
   Prefer Integrant keys, refs, and module expansion so backends swap through configuration.
5. Keep lifecycle concerns explicit.
   Resource ownership, startup, shutdown, retries, and failure boundaries must be obvious from the component graph.
6. Prefer boring extension points.
   Reuse existing patterns and naming unless there is a strong reason to introduce a new shape.

## D-Core Architecture Rules

### 1. Keep d-core focused on infrastructure

- d-core should provide reusable backend building blocks, not service-specific business rules.
- Put domain policy, routing decisions specific to one service, and app workflows outside d-core unless they are intentionally reusable infrastructure.
- If the proposal mainly serves one endpoint or one business flow, challenge whether it belongs here.

### 2. Design around stable protocols

- Protocols define the portable API surface for an abstraction.
- Keep protocol arguments generic: primitives, maps, collections, or records already internal to d-core.
- Put backend-specific knobs in the last `opts` argument.
- Do not leak vendor SDK objects or transport-specific data into the protocol surface unless that is the abstraction itself.
- Add a protocol only when multiple implementations or a stable seam are likely. Do not add a protocol just to wrap one function.

### 3. Use components to implement the abstraction

- Components should implement a protocol and own the integration logic for one concrete backend or shared orchestration layer.
- Components should depend on explicit inputs such as clients, codecs, routing, loggers, or other components.
- Keep records small and dependency-driven.
- Keep pure transformation, validation, and normalization functions outside `ig/init-key` when possible.
- `ig/init-key` should assemble the component, not bury business logic.

### 4. Keep clients thin and external-facing

- Clients are the lowest layer and should be the only place that speaks directly to vendor libraries or remote APIs.
- A client should primarily create, configure, and close connections or wrap raw SDK calls.
- Do not push orchestration, schema validation, or cross-backend coordination into client namespaces.
- When a resource has a real lifecycle, give the client its own Integrant key and `ig/halt-key!`.

### 5. Prefer config-first wiring

- Choose implementations in `duct.edn` and Integrant refs instead of branching on environment in code.
- Use modules to expand higher-level config into concrete keys when that reduces repetition.
- Keep runtime state out of config maps and module output.
- Make dependencies visible from the Integrant graph.

### 6. Keep extension paths replaceable

- Prefer patterns where a new backend means:
  1. add a client if needed
  2. add a component implementing the stable protocol
  3. wire it through config
- Avoid designs that require touching business logic to swap infrastructure.

## Layering Heuristics

### Add a protocol when

- the capability is meaningful across backends
- the call shape can stay portable
- the consuming code should not care which backend is used

### Add a client when

- the code must manage a connection or vendor SDK
- vendor-specific setup or teardown exists
- isolating the low-level API reduces leakage into higher layers

### Add a component when

- the abstraction needs Integrant wiring
- the implementation depends on clients or other components
- lifecycle or operational behavior matters

### Add a module when

- several keys should be derived from one higher-level config entry
- the wiring pattern will be reused
- expanding config reduces copy-paste without hiding runtime dependencies

### Avoid a new layer when

- a pure function or helper namespace is enough
- the abstraction only wraps one call with no stable seam
- the design would fragment the Integrant graph without operational value

## Output Format

When asked to design or propose architecture, structure the response in this order:

1. Problem boundary
2. Proposed abstraction shape
3. Protocol surface
4. Component and client layout
5. Config and Integrant wiring
6. Risks, tradeoffs, and why this belongs in d-core

For each proposal:

- Name the namespaces or files you would introduce or change.
- Explain why each responsibility belongs at that layer.
- Keep the design incremental and compatible with the repo's existing patterns.

If the right answer is "do not add a new d-core abstraction," say that explicitly and propose the smallest alternative.

## Review Stance

- Favor simplicity over framework cleverness.
- Favor explicit dependencies over hidden lookups.
- Favor replaceable backends over hard-coded vendor behavior.
- Favor designs that are easy to test in isolation and safe to operate in production.
