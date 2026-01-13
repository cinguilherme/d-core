(ns d-core.core.messaging.dead-letter.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defrecord CommonDeadLetter [default-sink-key sinks logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [this envelope error-info opts]
    (let [sink-key (or (:sink opts) default-sink-key)
          delegate (get sinks sink-key)]
      (if delegate
        (do
          (logger/log logger :info ::sending-to-dlq {:sink sink-key})
          (let [res (dl/send-dead-letter! delegate envelope error-info opts)]
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
  [_ {:keys [default-sink sinks logger] :or {default-sink :logger}}]
  (->CommonDeadLetter default-sink sinks logger))

