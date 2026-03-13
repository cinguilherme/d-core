# ADR 0003: Tile38 Geofencing Contract

Date: 2026-03-11
Status: Proposed

## Context

D-core already has a geo data-plane abstraction backed by Tile38 for CRUD and
query operations. What it does not yet have is a geofence control plane:

- a protocol for registering and managing fences
- canonical schemas for tracked objects, fence definitions, and normalized events
- a validation matrix that proves Tile38 behavior where the public docs are
  incomplete or internally inconsistent

The immediate goal is not to support multiple backends. The goal is to define a
clean D-core contract while using Tile38 exclusively for v1.

## Decision Drivers

- Keep tracked object CRUD on the existing geo abstraction.
- Keep business logic app-owned; geofencing stops at spatial/proximity events.
- Support both static fences and roaming proximity use cases.
- Support typed actors such as driver/passenger/audit without baking those
  domain terms into the library.
- Support circular and multi-shape geofences.
- Define the validation matrix up front so uncertain Tile38 behaviors are proven
  by tests before the public API is frozen.

## Decision

Add a dedicated geofence control-plane protocol and canonical Malli schemas:

- `d-core.core.geofence.protocol/GeofenceProtocol`
- `d-core.core.geofence.schema/*`

The contract is split as follows:

- Geo data plane: tracked object CRUD remains on
  `d-core.core.geo.protocol/GeoIndexProtocol`
- Geofence control plane: fence create/update/delete/list/test lives on
  `GeofenceProtocol`
- Event plane: raw Tile38 hook payloads are normalized into a stable D-core
  event schema before downstream publication/handling

## Canonical Concepts

### Tracked object

A tracked object is written to the geo index and includes:

- `:key` Tile38 collection key
- `:id` patternable identifier
- `:geometry` point, bounds, or GeoJSON
- optional `:fields`
- optional `:ttl-seconds`

### Fence

Two fence kinds are supported:

- `:static`
  - source key and optional source filters
  - area/shape (`:circle`, `:bounds`, `:geojson`, or `:shape-ref`)
  - operation (`:nearby`, `:within`, `:intersects`)
  - detect set (`:inside`, `:outside`, `:enter`, `:exit`, `:cross`)
- `:roam`
  - source key and optional source match/filter
  - target key, target pattern, and radius
  - optional `:no-dwell?`
  - detect set limited to `#{:roam}` until stronger behavior is validated

### Normalized event

All backend hook payloads normalize into an event that includes:

- `:backend`
- `:fence-id`
- `:fence-kind`
- `:detect`
- optional `:command`
- `:subject`
- optional `:counterparty` for roaming/candidate-style events
- optional backend/provider metadata

## Typed actor strategy

The library should support both:

- coarse actor typing through a patternable id namespace such as
  `driver:123`, `passenger:456`, `audit:789`
- richer attributes through object fields such as
  `{:kind "driver" :status "available"}`

Reason:

- Tile38 field filtering is useful for static fences and downstream policy
- roaming target selection is pattern-shaped (`ROAM key pattern meters`), so
  fields alone are not sufficient for all pairwise proximity use cases

## Multi-shape strategy

V1 supports multi-shape fences through GeoJSON:

- `MultiPolygon`
- `GeometryCollection`
- `FeatureCollection`

Mixed-shape unions that include circles are a library concern. When needed, the
library should compile them into GeoJSON before sending them to Tile38.

## V1 Tile38 Commands

### Data-plane commands

- `SET`
- `GET`
- `DEL`
- `DROP`
- `SCAN`
- `NEARBY`
- `WITHIN`
- `INTERSECTS`

### Fence-control commands

- `SETHOOK`
- `DELHOOK`
- `HOOKS`
- `TEST`

### Deferred / optional for later

- `SETCHAN`
- `DELCHAN`
- `CHANS`

These are useful for dev/debug flows but are not required to ship the first
hook-based runtime.

## Validation Matrix (must be proven by tests)

### Static fence behavior

1. Circle fence emits `enter` and `exit` for point updates.
2. Circle fence emits expected behavior for `set`, `del`, and `drop`.
3. GeoJSON `MultiPolygon` fence behaves correctly with `WITHIN`.
4. GeoJSON `GeometryCollection` or `FeatureCollection` behaves correctly with
   `INTERSECTS`.
5. Source filtering works for:
   - field equality/range
   - expression filters
   - optional source `MATCH` patterns

### Roaming behavior

1. `SETHOOK ... NEARBY ... FENCE ROAM ...` is accepted and produces events.
2. Roaming payload shape is stable enough to normalize into the canonical event
   schema.
3. `NODWELL` suppresses repeated notifications as expected.
4. Source `MATCH` combined with `ROAM` works as expected, or is rejected clearly
   enough that the public API can avoid promising it.
5. Behavior for separation / far-away transitions is explicitly measured.

### Lifecycle and admin behavior

1. Re-registering the same fence id updates/replaces the hook predictably.
2. `DELHOOK` removes fences cleanly.
3. `HOOKS` returns enough data to implement `get-fence` and `list-fences`.
4. `TEST` can validate compiled fence definitions without permanent registration.

### Object freshness

1. `SET ... EX` expiry removes stale actors as expected.
2. If expiry triggers geofence notifications, the exact event contract is
   recorded. If it does not, the runtime documents that clearly.

### Delivery and normalization

1. Hook delivery to the chosen sink works with D-core's consumer path.
2. Raw Tile38 payloads normalize into the canonical event schema.
3. Invalid or unexpected payloads fail schema validation with actionable errors.

## Why not expose matchmaking semantics directly

D-core should emit spatial/proximity events only. Candidate scoring, eligibility
checks, assignment, or dating preference logic stays app-owned.

This keeps the library reusable for:

- dispatch
- dating/social proximity
- audit/monitoring
- generic spatial triggers

without hard-coding domain policy into the infrastructure layer.

## Open Questions

- Whether roaming supports first-class pairwise enter/exit semantics, or only a
  raw `:roam` event that D-core must interpret.
- Whether `SETHOOK` fully supports `MATCH` in combination with `ROAM`; the docs
  are inconsistent and must not be treated as definitive without tests.
- Whether object expiry generates geofence callbacks and, if so, with what
  payload semantics.

## Next Steps

- Implement Tile38 hook-management commands in the client.
- Add integration tests that prove the validation matrix above against the local
  docker-compose Tile38 service.
- Implement Tile38-backed `GeofenceProtocol`.
- Add event normalization from raw Tile38 hook payloads into the canonical event
  schema.
- After validation, document exact behavior and developer guidance with updated
  docstrings and usage docs.
