(ns d-core.core.messaging.dead-letter.sinks.storage
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [cheshire.core :as json]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defrecord StorageDeadLetter [storage logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info opts]
    (let [timestamp (System/currentTimeMillis)
          trace (get-in envelope [:metadata :trace])
          filename (str "dead-letters/dlq-" timestamp "-" (java.util.UUID/randomUUID) ".json")
          error-info (dl/normalize-error-info error-info)
          payload (json/generate-string {:envelope envelope
                                         :error-info error-info
                                         :trace trace})]
      (try
        (storage/storage-put storage filename payload opts)
        {:ok true :sink :storage :path filename}
        (catch Exception e
          (logger/log logger :error ::storage-dlq-failed {:error (.getMessage e) :trace trace})
          {:ok false :error (.getMessage e)})))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter/storage
  [_ {:keys [storage logger]}]
  (->StorageDeadLetter storage logger))

