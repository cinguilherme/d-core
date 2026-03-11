# Copilot Instructions for d-core

## Repository purpose

This repository is `d-core`, a Duct and Integrant battery pack for reusable backend infrastructure.
Prefer reusable infrastructure abstractions over service-specific business logic.

## Architectural defaults

- Follow the standard d-core layering:
  1. stable protocol
  2. component implementing that protocol
  3. client wrapping the external system
  4. Duct and Integrant wiring through config
- Keep protocols portable. Protocol methods should accept Clojure data, not vendor SDK objects.
- Put backend-specific controls in the final `opts` argument.
- Keep clients thin and vendor-facing.
- Keep components explicit about dependencies and lifecycle.
- Prefer config-first wiring through Integrant refs and Duct modules instead of environment branching in code.
- Do not mix runtime state into static config structures.

## What belongs in d-core

- Reusable infrastructure concerns such as cache, storage, messaging, auth, rate limiting, stream backends, codecs, metrics, and clients.
- Shared orchestration logic that is reusable across services and backends.

## What does not belong in d-core

- Service-specific business rules
- Endpoint-specific behavior
- Product or domain policy that is not intended to be reused as infrastructure

If a task mainly serves one application flow, challenge whether it belongs here.

## Coding rules

- Prefer simple, explicit functions over unnecessary abstraction.
- Avoid deep nesting. If logic needs multiple nested `let` blocks or branches, extract helper functions. This is CRITICAL.
- If possible, preffer threading macros for linearizing complex transformations or decision logic.
- Prefer pure functions for transformation, validation, normalization, and decision logic.
- Keep `ig/init-key` focused on assembly and lifecycle, not business logic.
- Avoid hidden side effects at namespace load time.
- Make dependencies explicit in function arguments, records, and Integrant config.

## Integrant and Duct expectations

- New infrastructure should fit existing namespace and Integrant key conventions.
- Add `ig/halt-key!` when a resource has a real lifecycle.
- Prefer a small number of meaningful components over many thin wrappers.
- Use a Duct module only when one higher-level config entry should expand into several explicit Integrant keys.

## Testing expectations

- Add or update tests for behavior changes.
- Prefer unit tests for pure logic and focused integration tests for lifecycle or I/O boundaries.
- Do not require full system boot for logic that can be tested in isolation.
- Run targeted tests while iterating when feasible.

## Change guidance

When proposing or implementing changes:

- explain where the code should live
- preserve the protocol to component to client separation
- keep changes incremental
- prefer the smallest useful abstraction
- call out tradeoffs when introducing a new protocol, component, or module

## Reviews and analysis

When reviewing code in this repository:

- prioritize correctness, simplicity, lifecycle safety, testability, operational safety, performance, and cost awareness
- avoid broad rewrites unless the current design is clearly blocking correctness or maintainability
- give concrete, repository-shaped suggestions rather than generic advice

## Skills

If project skills are available for the current task, use them for specialized review or design work.
Use repository-wide instructions for baseline behavior and skills for detailed workflows.
