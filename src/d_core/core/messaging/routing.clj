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
      (first (get-in routing [:topics topic :sources]))
      (default-source routing)))

(defn sources-for-topic
  "Returns a vector of producer/source keys for `topic`.
  Prefers explicit :sources, falls back to single :source, then defaults."
  [routing topic]
  (let [sources (or (get-in routing [:topics topic :sources])
                    (get-in routing [:topics topic :source])
                    (default-source routing))]
    (vec (if (sequential? sources) sources [sources]))))

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
