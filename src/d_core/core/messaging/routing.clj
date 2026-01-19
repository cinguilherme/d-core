(ns d-core.core.messaging.routing
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :d-core.core.messaging/routing
  [_ routing]
  routing)

(defn topic-config
  [routing topic]
  (get-in routing [:topics topic]))

(defn publish-config
  [routing topic]
  (get-in routing [:publish topic]))

(defn publish-targets
  "Returns a vector of publish targets for `topic` (or empty when unset)."
  [routing topic]
  (let [targets (get-in routing [:publish topic :targets])]
    (cond
      (nil? targets) []
      (sequential? targets) (vec targets)
      :else [targets])))

(defn subscription-config
  [routing subscription-id]
  (get-in routing [:subscriptions subscription-id]))

;; Dead letter configuration
;;
;; The routing component can define dead letter configuration at three levels:
;; - `[:defaults :deadletter {...}]` (global defaults)
;; - `[:topics <topic> :deadletter {...}]` (per-topic overrides)
;; - `[:subscriptions <id> :deadletter {...}]` (per-subscription overrides)
;;
;; The dead-letter subsystem uses these settings to choose sink/policy/limits.
(defn default-deadletter
  [routing]
  (get-in routing [:defaults :deadletter] {}))

(defn deadletter-config
  "Returns the effective dead-letter configuration for `topic` by merging routing
  defaults with topic overrides and subscription overrides (when provided)."
  ([routing topic]
   (deadletter-config routing topic nil))
  ([routing topic subscription-id]
   (merge (default-deadletter routing)
          (get-in routing [:topics topic :deadletter] {})
          (when subscription-id
            (get-in routing [:subscriptions subscription-id :deadletter] {})))))
