(ns d-core.core.messaging.dead-letter.sinks.producer
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.producers.protocol :as producer]
            [d-core.tracing :as tracing]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defrecord ProducerDeadLetter [producer dlq-topic max-retries delay-ms logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info opts]
    (let [dlq-topic (or (:dlq-topic opts) dlq-topic "dead-letters")
          delay-ms (or (:delay-ms opts) delay-ms 0)
          max-retries (or (:max-retries opts) max-retries 3)
          trace (get-in envelope [:metadata :trace])
          parent (tracing/decode-ctx trace)
          dlq-ctx (tracing/child-ctx parent)
          ;; Retry count is stored on the envelope to avoid infinite loops.
          current-retry (get-in envelope [:msg :retry-count] 0)
          next-retry (inc current-retry)
          error-info (dl/normalize-error-info error-info)]
      (if (> next-retry max-retries)
        (do
          (logger/log logger :error ::max-retries-exceeded
                      {:topic dlq-topic :retry-count current-retry :trace trace})
          {:ok false :error :max-retries-exceeded})
        (let [payload {:original-envelope envelope
                       :error-info error-info
                       :retry-count next-retry
                       :failed-at (System/currentTimeMillis)}]
          (try
            (if (> delay-ms 0)
              (do
                (logger/log logger :info ::delaying-dead-letter
                            {:delay-ms delay-ms :retry-count next-retry :trace trace})
                (future
                  (Thread/sleep delay-ms)
                  (producer/produce! producer payload {:topic dlq-topic :trace/ctx dlq-ctx})))
              (producer/produce! producer payload {:topic dlq-topic :trace/ctx dlq-ctx}))
            {:ok true
             :sink :producer
             :topic dlq-topic
             :retry-count next-retry
             :trace (tracing/encode-ctx dlq-ctx)}
            (catch Exception e
              (logger/log logger :error ::producer-dlq-failed {:error (.getMessage e) :trace trace})
              {:ok false :error (.getMessage e)})))))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter/producer
  [_ {:keys [producer dlq-topic max-retries delay-ms logger]
      :or {dlq-topic "dead-letters"
           max-retries 3
           delay-ms 0}}]
  (->ProducerDeadLetter producer dlq-topic max-retries delay-ms logger))

