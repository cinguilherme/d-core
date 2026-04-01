(ns d-core.core.geocoding.protocol)

(defprotocol GeocodingProtocol
  "Provider-neutral geocoding contract.

  `query` and `location` are plain maps validated by
  `d-core.core.geocoding.schema`.

  `opts` is reserved for transport or provider-specific request controls that
  should not leak into the shared input contract."
  (geocode [this query opts]
    "Resolve an address or free-form search into zero or more normalized
     location matches.")
  (reverse-geocode [this location opts]
    "Resolve a coordinate pair into zero or one normalized address matches."))
