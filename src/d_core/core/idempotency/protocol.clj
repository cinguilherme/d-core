(ns d-core.core.idempotency.protocol)

(defprotocol IdempotencyProtocol
  (claim! [this key ttl-ms] "Claim an idempotency key. Returns map with :status in #{:claimed :in-progress :completed}.")
  (complete! [this key response ttl-ms] "Store final response for an idempotency key and release pending state.")
  (lookup [this key] "Lookup previously completed idempotency response."))
