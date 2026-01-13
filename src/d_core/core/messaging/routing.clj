(ns d-core.core.messaging.routing
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :d-core.core.messaging/routing
  [_ routing]
  routing)

(defn topic-config
  [routing topic]
  (get-in routing [:topics topic]))

(defn default-source
  [routing]
  (get-in routing [:defaults :source] :in-memory))

(defn source-for-topic
  [routing topic]
  (or (get-in routing [:topics topic :source])
      (default-source routing)))

;; Dead letter configuration
;;
;; The routing component can define dead letter configuration at two levels:
;; - `[:defaults :deadletter {...}]` (global defaults)
;; - `[:topics <topic> :deadletter {...}]` (per-topic overrides)
;;
;; The dead-letter subsystem uses these settings to choose sink/policy/limits.
(defn default-deadletter
  [routing]
  (get-in routing [:defaults :deadletter] {}))

(defn deadletter-config
  "Returns the effective dead-letter configuration for `topic` by merging routing
  defaults with topic overrides."
  [routing topic]
  (merge (default-deadletter routing)
         (get-in routing [:topics topic :deadletter] {})))
