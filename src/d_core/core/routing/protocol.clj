(ns d-core.core.routing.protocol)

(defprotocol RoutingProtocol
  "Provider-neutral routing and matrix contract.

  `request` is a plain map validated by `d-core.core.routing.schema`.

  `opts` is reserved for transport or provider-specific request controls that
  should not leak into the shared input contract."
  (route [this request opts]
    "Resolve an ordered set of locations into zero or more normalized routes.")
  (matrix [this request opts]
    "Resolve source and target locations into a normalized time-distance matrix."))
