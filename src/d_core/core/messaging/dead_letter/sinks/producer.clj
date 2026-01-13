(ns d-core.core.messaging.dead-letter.sinks.producer
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.producers.protocol :as producer]
            [d-core.core.messaging.routing :as routing]
            [d-core.tracing :as tracing]
            [d-core.core.messaging.dead-letter.destination :as dest]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defrecord ProducerDeadLetter [producer routing delay-ms logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info opts]
    (let [opts (or opts {})
          delay-ms (or (:delay-ms opts) delay-ms 0)
          trace (get-in envelope [:metadata :trace])
          parent (tracing/decode-ctx trace)
          dlq-ctx (tracing/child-ctx parent)
          error-info (dl/normalize-error-info error-info)
          orig-topic (or (get-in envelope [:metadata :dlq :topic]) :default)
          dlq (get-in envelope [:metadata :dlq] {})
          dlq-id (:id dlq)
          payload-hash (:payload-hash dlq)
          ;; Allow explicit override, otherwise derive destination using `.dl` convention.
          dlq-topic (or (:dlq-topic opts) (get-in envelope [:metadata :dlq :deadletter :dlq-topic]))
          dest (if dlq-topic
                 {:topic dlq-topic
                  :source (routing/source-for-topic routing orig-topic)
                  :options {}}
                 (dest/dlq-destination routing orig-topic envelope))
          produce-opts (merge {:topic (:topic dest)
                               ;; force same transport as original topic unless overridden
                               :source (:source dest)
                               :trace/ctx dlq-ctx}
                              (:options dest))
          payload {:dlq-id dlq-id
                   :payload-hash payload-hash
                   :original-topic orig-topic
                   :original-envelope envelope
                   :error-info error-info
                   :failed-at (System/currentTimeMillis)}]
      (try
        (if (> delay-ms 0)
          (do
            (logger/log logger :info ::delaying-dead-letter
                        {:delay-ms delay-ms :topic (:topic dest) :trace trace})
            (future
              (Thread/sleep delay-ms)
              (producer/produce! producer payload produce-opts)))
          (producer/produce! producer payload produce-opts))
        {:ok true
         :sink :producer
         :topic (:topic dest)
         :trace (tracing/encode-ctx dlq-ctx)}
        (catch Exception e
          (logger/log logger :error ::producer-dlq-failed {:error (.getMessage e) :trace trace})
          {:ok false :error (.getMessage e)})))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter/producer
  [_ {:keys [producer routing delay-ms logger]
      :or {delay-ms 0}}]
  (->ProducerDeadLetter producer routing delay-ms logger))

