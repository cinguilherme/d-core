(ns d-core.core.geofence.protocol)

(defprotocol GeofenceProtocol
  "Geofence control-plane and event-normalization contract.

  Tracked object CRUD stays on `d-core.core.geo.protocol/GeoIndexProtocol`.
  This protocol manages fence definitions, backend validation, and conversion of
  backend-specific hook payloads into D-core's normalized geofence event shape."
  (upsert-fence! [this fence opts]
    "Create or update a fence definition.

     `fence` must conform to `d-core.core.geofence.schema/fence-schema`.

     Returns a backend-specific result map. Implementations should treat the
     operation as idempotent for the same fence id/name when possible.")
  (get-fence [this fence-id opts]
    "Fetch a previously registered fence by id/name.

     Returns either a normalized fence map or a backend-specific description
     that can be normalized by the implementation.")
  (list-fences [this query opts]
    "List fences using backend-supported filtering.

     `query` is intentionally small and backend-agnostic. Implementations may
     support:
     - :prefix / :pattern for id-name filtering
     - :kind in #{:static :roam}
     - :limit

     Returns {:ok true :items [...]} or {:ok false :error ...}.")
  (delete-fence! [this fence-id opts]
    "Delete a fence by id/name.")
  (test-fence [this fence opts]
    "Validate how a fence compiles against the backend without permanently
     registering it. Tile38-backed implementations can use this to wrap `TEST`
     and command-shape validation.")
  (normalize-event [this payload opts]
    "Convert a raw backend event payload into the normalized D-core geofence
     event shape defined in `d-core.core.geofence.schema/event-schema`."))
