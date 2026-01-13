(ns d-core.core.messaging.dead-letter.policy
  "Dead-letter policy: classification + destination selection.

  Policies decide:
  - whether a dead-letter is eligible for auto replay vs stuck/poison/manual
  - which sink to use (logger/storage/producer/hybrid)
  - which destination to publish to (e.g. retry vs stuck DLQ topics)
  - limits like :max-attempts / :delay-ms

  The effective per-topic configuration is expected to be present on the envelope at:
  - `[:metadata :dlq :deadletter]`
  (populated by consumer runtimes)."
  (:require [integrant.core :as ig]))

(defprotocol DeadLetterPolicy
  (classify [this envelope error-info opts]
    "Return a decision map.

    Must return:
    - `:status` keyword
    - `:sink` keyword

    May return:
    - `:max-attempts` int
    - `:delay-ms` int
    - `:dlq-topic` keyword (for producer sink)
    - `:dlq-topics` map keyword->keyword (e.g. {:retry :dlq-retry ...})"))

(defn- dlq-meta
  [envelope]
  (get-in envelope [:metadata :dlq] {}))

(defn- dlq-cfg
  [envelope]
  (get-in envelope [:metadata :dlq :deadletter] {}))

(defrecord DefaultPolicy []
  DeadLetterPolicy
  (classify [_ envelope _error-info opts]
    (let [cfg (dlq-cfg envelope)
          meta (dlq-meta envelope)
          ;; attempt is maintained by the replay controller; on first failure it is 0.
          attempt (long (or (:attempt meta) 0))
          max-attempts (long (or (:max-attempts cfg) 3))
          delay-ms (long (or (:delay-ms cfg) 0))
          ;; sink can be controlled per topic; opts can override (e.g. manual forcing).
          sink (or (:sink opts) (:sink cfg))
          sink (or sink :hybrid)
          ;; destination topics are per status; defaults work without extra routing.
          topics (merge {:retry :dlq-retry
                         :stuck :dlq-stuck
                         :poison :dlq-poison
                         :manual :dlq-manual}
                        (:dlq-topics cfg))
          status0 (or (:status cfg) (:status meta) :eligible)
          status (if (and (= status0 :eligible) (>= attempt max-attempts))
                   :stuck
                   status0)
          dlq-topic (case status
                      :poison (:poison topics)
                      :manual (:manual topics)
                      :stuck (:stuck topics)
                      ;; eligible/default
                      (:retry topics))]
      {:status status
       :sink sink
       :max-attempts max-attempts
       :delay-ms delay-ms
       :dlq-topic dlq-topic
       :dlq-topics topics})))

(defmethod ig/init-key :d-core.core.messaging.dead-letter.policy/default
  [_ _cfg]
  (->DefaultPolicy))

