(ns d-core.core.messaging.dead-letter.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.messaging.dead-letter.policy :as policy]
            [d-core.core.messaging.dead-letter.sinks.hybrid :as hybrid]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defn- assoc-dlq-policy
  "Attach policy decisions onto the envelope DLQ metadata so sinks can persist it."
  [envelope {:keys [status max-attempts delay-ms dlq-topic]}]
  (cond-> envelope
    status (assoc-in [:metadata :dlq :status] status)
    max-attempts (assoc-in [:metadata :dlq :max-attempts] max-attempts)
    delay-ms (assoc-in [:metadata :dlq :delay-ms] delay-ms)
    dlq-topic (assoc-in [:metadata :dlq :dlq-topic] dlq-topic)))

(defn- compute-default-sink
  [sinks]
  (cond
    (contains? sinks :hybrid) :hybrid
    (contains? sinks :storage) :storage
    (contains? sinks :producer) :producer
    :else :logger))

(defrecord CommonDeadLetter [default-sink-key sinks policy logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [this envelope error-info opts]
    (let [opts (or opts {})
          decision (when policy (policy/classify policy envelope error-info opts))
          ;; Apply policy decision to envelope for persistence/traceability.
          envelope (if decision (assoc-dlq-policy envelope decision) envelope)
          ;; sink selection order: explicit opts override > policy > default.
          sink-key (or (:sink opts)
                       (:sink decision)
                       default-sink-key)
          delegate (get sinks sink-key)]
      (if delegate
        (do
          (logger/log logger :info ::sending-to-dlq {:sink sink-key})
          (let [opts (cond-> opts
                       ;; policy can choose a per-status dlq-topic for producer-like sinks
                       (:dlq-topic decision) (assoc :dlq-topic (:dlq-topic decision))
                       (:delay-ms decision) (assoc :delay-ms (:delay-ms decision)))
                res (dl/send-dead-letter! delegate envelope error-info opts)]
            (if (and (not (:ok res)) (= sink-key :producer))
              ;; If producer fails (e.g. retries exceeded), fallback to logger so the message isn't lost.
              (dl/send-dead-letter! this
                                    envelope
                                    (assoc error-info :dlq-error (:error res))
                                    (assoc opts :sink :logger))
              res)))
        (do
          (logger/log logger :error ::missing-dlq-sink {:sink sink-key})
          {:ok false :error :missing-sink})))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter/common
  [_ {:keys [default-sink sinks policy logger]}]
  (let [sinks (or sinks {})
        ;; Auto-create a hybrid sink when both storage+producer exist, unless explicitly provided.
        sinks (if (and (not (contains? sinks :hybrid))
                       (contains? sinks :storage)
                       (contains? sinks :producer))
                (assoc sinks :hybrid (hybrid/make-hybrid {:storage-sink (:storage sinks)
                                                         :producer-sink (:producer sinks)
                                                         :logger logger}))
                sinks)
        default-sink (or default-sink (compute-default-sink sinks))]
    (->CommonDeadLetter default-sink sinks policy logger)))

