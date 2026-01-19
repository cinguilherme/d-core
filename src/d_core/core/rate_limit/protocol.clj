(ns d-core.core.rate-limit.protocol)

(defprotocol RateLimitProtocol
  (consume! [this key opts]
    "Attempt to consume from the rate limit.
     Returns {:allowed? boolean
              :remaining long
              :reset-at epoch-ms|nil
              :retry-after-ms long|nil}.")
  (consume-blocking! [this key opts]
    "Blocking variant of consume! that waits until allowed when possible."))
