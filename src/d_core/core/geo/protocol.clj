(ns d-core.core.geo.protocol)

(defprotocol GeoIndexProtocol
  (set-point! [this key id point opts]
    "Stores a point at key/id (point is [lat lon] or {:lat :lon}).")
  (set-object! [this key id object opts]
    "Stores a GeoJSON object at key/id (object is map or JSON string).")
  (set-bounds! [this key id bounds opts]
    "Stores a bounds object at key/id (bounds is [minlat minlon maxlat maxlon]).")
  (get-object [this key id opts]
    "Fetches an object by key/id. Use opts for POINT/OBJECT/BOUNDS/HASH.")
  (delete! [this key ids opts]
    "Deletes one or more ids from a key.")
  (drop! [this key opts]
    "Drops an entire key/index.")
  (scan [this key opts]
    "Scans ids from a key.")
  (nearby [this key point opts]
    "Finds nearby objects relative to a point.")
  (within [this key shape opts]
    "Finds objects within a shape (bounds/object/point).")
  (intersects [this key shape opts]
    "Finds objects intersecting a shape (bounds/object/point)."))
