# ADR 0005: Routing and Matrix Protocol v1 (OSRM + Valhalla)

Date: 2026-04-01
Status: Proposed

## Context

After introducing geocoding in D-core, consuming services still need reusable
routing and matrix capabilities without coupling business logic to a specific
provider wire format.

Requirements for this increment:

- one shared protocol for both route and matrix operations
- first-party open routing providers for local/self-hosted workflows
- no service-level provider fallback strategy in D-core

## Decision

Introduce routing as a dedicated D-core capability:

- `d-core.core.routing.protocol/RoutingProtocol`
  - `route`
  - `matrix`

First implementation targets:

- OSRM router (`:d-core.core.routing.osrm/router`)
- Valhalla router (`:d-core.core.routing.valhalla/router`)

Both implementations:

- depend on `:d-core.core.http/client`
- validate strict provider-neutral request schemas
- normalize provider payloads into shared route and matrix envelopes

Out of scope for this ADR:

- commercial provider integrations
- provider fallback orchestration and regional policy
- routing strategy composition wrappers

## Rationale

- Routing and matrix are infra-level capabilities reused across services.
- A single protocol avoids app-level rewrites when changing providers.
- OSRM and Valhalla give immediate self-hosted options for local and non-commercial workloads.
- Keeping fallback policy out of D-core preserves application ownership of
  product/cost tradeoffs.

## Consequences

- Services can target one stable contract while switching provider components by config.
- Local docker-compose can run OSRM and Valhalla with small Monaco defaults.
- Matrix no-path scenarios normalize to `nil` cells; route no-path scenarios
  normalize to empty route vectors.
- Future provider composition can be added in another ADR without changing
  the base protocol.
