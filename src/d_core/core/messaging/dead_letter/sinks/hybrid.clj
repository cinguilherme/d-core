(ns d-core.core.messaging.dead-letter.sinks.hybrid
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defrecord HybridDeadLetter [storage-sink producer-sink logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info opts]
    (let [storage-res (when storage-sink
                        (try
                          (dl/send-dead-letter! storage-sink envelope error-info opts)
                          (catch Exception e
                            {:ok false :error (.getMessage e)})))
          producer-res (when producer-sink
                         (try
                           (dl/send-dead-letter! producer-sink envelope error-info opts)
                           (catch Exception e
                             {:ok false :error (.getMessage e)})))
          ok? (boolean (or (:ok storage-res) (:ok producer-res)))]
      (when (and logger (not ok?))
        (logger/log logger :error ::hybrid-dlq-failed
                    {:storage storage-res
                     :producer producer-res
                     :trace (get-in envelope [:metadata :trace])}))
      {:ok ok?
       :sink :hybrid
       :sinks {:storage storage-res
               :producer producer-res}})))

(defn make-hybrid
  [{:keys [storage-sink producer-sink logger]}]
  (->HybridDeadLetter storage-sink producer-sink logger))

(defmethod ig/init-key :d-core.core.messaging.dead-letter/hybrid
  [_ {:keys [storage producer logger]}]
  ;; `storage` and `producer` are expected to be DeadLetterProtocol implementations,
  ;; typically refs to `:d-core.core.messaging.dead-letter/storage` and `/producer`.
  (->HybridDeadLetter storage producer logger))

