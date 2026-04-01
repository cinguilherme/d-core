# ADR 0004: Geocoding Protocol v1 (Nominatim + Cached Wrapper)

Date: 2026-04-01
Status: Proposed

## Context

D-core already ships reusable infrastructure protocols (cache, storage,
messaging, geo/geofence, etc.) while keeping business policy in consuming
services.

Fair-rides and similar services need geocoding from day zero, but:

- routing/ETA/matrix is a separate capability with different constraints
- provider choice and hybrid strategy are application policy
- public geocoding APIs can have strict rate and usage limits

## Decision

Introduce geocoding as a dedicated D-core capability:

- `d-core.core.geocoding.protocol/GeocodingProtocol`
  - `geocode`
  - `reverse-geocode`

First implementation target:

- Nominatim-backed geocoder (`:d-core.core.geocoding.nominatim/geocoder`)

Reusable composition in scope for v1:

- cached geocoder wrapper (`:d-core.core.geocoding.cached/geocoder`)

Out of scope for this ADR:

- routing / ETA / distance matrix
- provider fallback orchestration
- autocomplete and place-search UX policy

## Rationale

- Geocoding belongs in D-core because it is infra-level and reusable across
  services.
- Routing is deferred to keep this increment focused and shippable.
- Provider strategy (single, fallback, region-based, cost-tiered) remains
  service-owned.
- Nominatim enables immediate low-cost local development with self-hosted
  deployment paths.
- Cached composition is broadly reusable and reduces external dependency load.

## Consequences

- Services can adopt a stable geocoding contract independent of provider wire
  shape.
- Local development can use docker-compose Nominatim with a tiny Monaco extract
  by default.
- Public endpoint usage is possible but should be conservative (rate-limited and
  cached).
- Multi-provider fallback remains open for a future ADR.
