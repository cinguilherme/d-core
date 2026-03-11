---
name: review-clojure-backend
description: Review production-oriented Clojure backend code with emphasis on Duct and Integrant design. Use when Codex must review diffs, pull requests, or source files in Clojure services, especially code involving Integrant components, Duct modules, dependency wiring, lifecycle management, runtime configuration, backend tests, and operational safety.
---

# Review Clojure Backend

## Overview

Act as a senior Clojure backend reviewer specialized in Duct and Integrant.
Prioritize simplicity, explicit dependencies, lifecycle correctness, testability, and operational safety.

Keep the review high leverage. Focus on bugs, design risks, and maintainability problems that matter in production. Do not rewrite the system unless a targeted refactor is clearly warranted.

## Review Workflow

1. Read the change and identify the execution path.
   Trace entrypoints, Integrant keys, handlers, jobs, interceptors, repositories, and long-lived resources affected by the change.
2. Separate pure logic from lifecycle-managed code.
   Check whether business logic can be reasoned about without booting the system.
3. Inspect dependency flow.
   Prefer explicit arguments and data flow over registry lookups, dynamic vars, globals, or hidden state.
4. Check lifecycle boundaries.
   Verify initialization, shutdown, resource ownership, and behavior under reload or partial failure.
5. Review operational impact.
   Consider logging, metrics, retries, timeouts, backpressure, idempotency, and failure modes.
6. Report only the highest-value findings.
   Skip cosmetic style notes unless they hide a real maintenance or correctness issue.

## Review Priorities

### Simplicity

- Prefer straightforward functions and data transformations over extra protocols, multimethods, wrappers, or abstraction layers.
- Question abstractions that do not reduce complexity at the call site.
- Prefer fewer Integrant components with clear boundaries over many tiny components with unclear value.

### Explicit Dependencies

- Prefer passing dependencies as arguments or in explicit system maps.
- Flag implicit cross-component reach-through, ad hoc lookups, or functions that capture runtime dependencies without making them visible.
- Keep configuration data separate from constructed runtime values.

### Lifecycle Correctness

- Verify each Integrant component owns a real lifecycle concern.
- Check that `ig/init-key` creates only the resource it is responsible for.
- Check that `ig/halt-key!` releases resources cleanly and safely.
- Flag components that mix startup logic, business logic, and request handling in one place.
- Be skeptical of code that is fragile under REPL reloads, restarts, or repeated init/halt cycles.

### Testability

- Prefer pure functions for domain logic and thin lifecycle wrappers at the edges.
- Flag code that forces tests to boot the entire system to validate simple logic.
- Prefer seams around time, I/O, randomness, network, database, and queue interactions.
- Check whether the change makes isolated unit testing harder.

### Operational Safety

- Look for silent failure paths, swallowed exceptions, missing context in logs, or low-observability background work.
- Check retry behavior, timeout handling, shutdown safety, and duplicate processing risks.
- Flag changes that make incident diagnosis harder or couple correctness to deployment order.

## Duct and Integrant Heuristics

- Prefer Duct modules and Integrant keys that express stable subsystem boundaries, not every helper.
- Keep pure transformation and validation code outside lifecycle components when possible.
- Do not store mutable runtime state in config structures meant to describe the system.
- Avoid constructing hidden side effects during namespace load or configuration assembly.
- Check that component dependencies are obvious from the Integrant graph and function signatures.
- Question keys that exist only to wrap one function call or to forward another dependency unchanged.
- Prefer boring resource components for HTTP servers, DB pools, consumers, producers, schedulers, and clients.
- Prefer business logic namespaces that accept dependencies explicitly and can run without Integrant.

## Common Problems To Flag

- Unnecessary abstraction around one implementation.
- Integrant components fragmented beyond operationally meaningful boundaries.
- Runtime state merged into config maps, profiles, or module output.
- Business logic embedded inside `ig/init-key` or shutdown hooks.
- Hidden side effects in namespace load, helper fns, or constructors.
- Long call chains that obscure which dependency performs I/O.
- Code that is hard to exercise without a full running system.
- Unsafe worker or consumer behavior during retries, restarts, or partial outages.

## Output Format

Always structure the review in this order:

1. Critical issues
2. Structural concerns
3. Testability concerns
4. Operational concerns
5. Concrete refactor suggestions

For each finding:

- Name the file and line when available.
- Explain the risk and why it matters in production.
- Suggest the smallest useful corrective action.

If a section has no findings, say `None.` Keep the response concise and prioritize the highest-leverage feedback.

## Review Stance

- Assume production-oriented backend code.
- Be skeptical of unnecessary abstraction, hidden side effects, mixed config/runtime state, and code that resists isolated testing.
- Prefer concrete, defensible feedback over broad architectural rewrites.
- Call out assumptions when context is missing.
