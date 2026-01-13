(ns d-core.core.messaging.dead-letter
  "Dead letter support (public facade).

  This namespace is intentionally small and stable:
  - It provides the public API (`DeadLetterProtocol`, `send-dead-letter!`, helpers)
  - It *requires* all built-in sink implementations so Integrant can resolve their keys

  ## Protocol
  - `d-core.core.messaging.dead-letter.protocol`

  ## Built-in implementations
  - Logger sink: `d-core.core.messaging.dead-letter.sinks.logger`
  - Storage sink: `d-core.core.messaging.dead-letter.sinks.storage`
  - Producer sink: `d-core.core.messaging.dead-letter.sinks.producer`
  - Common facade (sink selection/fallback): `d-core.core.messaging.dead-letter.common`

  ## Extending
  Add a new sink under `d-core.core.messaging.dead-letter.sinks.*` and implement
  `DeadLetterProtocol`. If you want it to be wired via Integrant, define a
  `(defmethod ig/init-key ...)` in that namespace and ensure it gets required
  (either by requiring this facade, or by requiring your sink namespace directly)."
  (:require [d-core.core.messaging.dead-letter.protocol :as protocol]
            [d-core.core.messaging.dead-letter.policy]
            [d-core.core.messaging.dead-letter.metadata]
            [d-core.core.messaging.dead-letter.destination]
            ;; Side-effect requires: register Integrant `init-key` methods.
            [d-core.core.messaging.dead-letter.sinks.logger]
            [d-core.core.messaging.dead-letter.sinks.storage]
            [d-core.core.messaging.dead-letter.sinks.producer]
            [d-core.core.messaging.dead-letter.sinks.hybrid]
            [d-core.core.messaging.dead-letter.replay.redis]
            [d-core.core.messaging.dead-letter.admin.storage]
            [d-core.core.messaging.dead-letter.common]))

;; Public protocol + helpers (re-exported for backwards compatibility).
(def DeadLetterProtocol protocol/DeadLetterProtocol)
(def send-dead-letter! protocol/send-dead-letter!)
(def normalize-error-info protocol/normalize-error-info)
