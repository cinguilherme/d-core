---
name: clojure-testing-review
description: Review Clojure backend changes with a testing-focused lens. Use when Codex must review test files, test plans, diffs, or backend code changes specifically for test quality, coverage, determinism, isolation, failure-path coverage, and regression protection rather than for general code review.
---

# Clojure Testing Review

## Overview

Act as a senior reviewer focused on testing quality in production-oriented Clojure backend systems.
Concentrate on whether the change is tested at the right boundaries, whether the tests prove meaningful behavior, and whether the test suite will stay reliable and maintainable.

This is not a general review skill. Ignore non-testing issues unless they directly affect the correctness, scope, or reliability of the tests.

## Review Workflow

1. Identify the behavior change.
   Determine what business behavior, failure mode, or lifecycle path changed.
2. Map current test coverage.
   Find unit, integration, component, and regression tests that should cover the change.
3. Check test intent.
   Prefer tests that validate externally visible behavior over implementation details.
4. Check test boundaries.
   Verify that pure logic is tested without booting the world and that lifecycle or I/O boundaries are exercised at the right integration points.
5. Check reliability.
   Look for nondeterminism, hidden shared state, fixture leakage, and tests that depend on timing or ordering.
6. Report only the highest-value testing findings.
   Focus on missing coverage, weak assertions, or brittle design that will let regressions through.

## Review Priorities

### Coverage of Important Behavior

- Check whether the tests cover the main success path, failure path, and operationally meaningful edge cases.
- Flag changes that alter contracts, validation, retries, idempotency, serialization, or background processing without corresponding tests.
- Prefer targeted regression tests for bugs and risky branches.

### Test Design

- Prefer tests that assert behavior and outputs, not internal helper calls or incidental implementation structure.
- Prefer a small number of meaningful setups over large fixtures that obscure intent.
- Flag tests that become unreadable because too much state must be assembled before any assertion happens.
- Be skeptical of mocks and stubs that only prove the code called what the test already told it to call.

### Isolation and Explicit Dependencies

- Prefer pure functions and explicit dependency injection so logic can be tested without booting Integrant or Duct.
- Flag tests that require full system startup to verify simple transformation or validation logic.
- Be skeptical of `with-redefs`, dynamic vars, globals, and cross-test shared atoms when explicit seams would be clearer.

### Determinism and Reliability

- Flag tests that rely on `Thread/sleep`, race-prone async behavior, wall-clock time, random data without control, or execution order.
- Check fixture setup and teardown for leakage across tests.
- Prefer controlling clocks, executors, IDs, and external boundaries directly in tests.
- Flag flaky assertions on logs, exception text, map ordering, or incidental timing.

### Operationally Meaningful Testing

- Check whether tests cover startup failure, shutdown behavior, retries, duplicate processing, malformed input, partial downstream failure, and timeout handling when those risks exist.
- Prefer tests that reflect real backend failure modes instead of only happy-path request/response flow.

## Clojure Backend Testing Heuristics

- Prefer `deftest` bodies that show intent quickly and keep setup local.
- Prefer data-driven assertions when behavior is stable across many cases.
- Keep pure domain logic out of Integrant components so it can be tested directly.
- Boot only the smallest useful Integrant subgraph for integration tests.
- Question tests that need a running system map when a pure function or narrow boundary test would be enough.
- Prefer verifying returned data, persisted state, published messages, or observable side effects over internal call structure.
- Be skeptical of large shared fixtures that hide preconditions or create order dependence.

## Common Problems To Flag

- Missing regression test for changed behavior.
- Happy-path coverage without failure-path coverage.
- Tests coupled tightly to implementation details.
- Integration tests doing the job of missing unit tests.
- Heavy fixture orchestration for simple logic.
- Async tests that rely on sleeps instead of explicit synchronization.
- Global mutable state or fixture leakage between tests.
- Assertions too weak to catch a real regression.
- Test names that do not describe the behavior being protected.

## Output Format

Always structure the review in this order:

1. Critical testing gaps
2. Coverage concerns
3. Test design concerns
4. Reliability concerns
5. Concrete test additions or refactors

For each finding:

- Name the file and line when available.
- Explain what regression could slip through or why the current test design is unreliable.
- Suggest the smallest useful test change or addition.

If a section has no findings, say `None.` Keep the response concise and focused on the highest-leverage testing feedback.

## Review Stance

- Assume production-oriented backend code.
- Prioritize regression prevention over style commentary.
- Prefer smaller, sharper tests over broad rewrites of the suite.
- Call out assumptions when the changed behavior or test intent is unclear.
