(ns d-core.core.messaging.dead-letter.sinks.logger
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defrecord LoggerDeadLetter [logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info _opts]
    (let [trace (get-in envelope [:metadata :trace])
          error-info (dl/normalize-error-info error-info)]
      (logger/log logger :error ::dead-letter-logged
                  {:envelope envelope
                   :error-info error-info
                   :trace trace})
      {:ok true :sink :logger})))

(defmethod ig/init-key :d-core.core.messaging.dead-letter/logger
  [_ {:keys [logger]}]
  (->LoggerDeadLetter logger))

