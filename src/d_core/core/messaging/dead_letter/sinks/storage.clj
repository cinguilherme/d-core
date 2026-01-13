(ns d-core.core.messaging.dead-letter.sinks.storage
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.messaging.dead-letter.protocol :as dl]))

(defrecord StorageDeadLetter [storage logger]
  dl/DeadLetterProtocol
  (send-dead-letter! [_ envelope error-info opts]
    (let [trace (get-in envelope [:metadata :trace])
          dlq (get-in envelope [:metadata :dlq] {})
          dlq-id (or (:id dlq) (str (java.util.UUID/randomUUID)))
          topic (or (:topic dlq) :unknown)
          topic-name (cond
                       (keyword? topic) (name topic)
                       (string? topic) topic
                       :else (str topic))
          ;; Storage is the admin source-of-truth:
          ;; - by-id enables O(1) fetch by dlq-id
          ;; - by-topic supports topic-scoped listing
          by-id-key (str "dead-letters/by-id/" dlq-id ".json")
          by-topic-key (str "dead-letters/by-topic/" topic-name "/" dlq-id ".json")
          ;; Indexes allow listing without requiring storage backend listing support.
          ;; This is a simple append-only JSONL file implemented as read+write.
          index-key (str "dead-letters/index/by-topic/" topic-name ".jsonl")
          index-all-key "dead-letters/index/all.jsonl"
          error-info (dl/normalize-error-info error-info)
          payload (json/generate-string {:dlq {:id dlq-id
                                               :payload-hash (:payload-hash dlq)
                                               :status (:status dlq)
                                               :attempt (:attempt dlq)
                                               :topic topic
                                               :subscription-id (:subscription-id dlq)
                                               :first-failed-at (:first-failed-at dlq)
                                               :last-failed-at (:last-failed-at dlq)}
                                         :envelope envelope
                                         :error-info error-info
                                         :trace trace})]
      (try
        (storage/storage-put storage by-id-key payload opts)
        (storage/storage-put storage by-topic-key payload opts)
        (let [idx-line (json/generate-string {:dlq-id dlq-id
                                              :payload-hash (:payload-hash dlq)
                                              :status (:status dlq)
                                              :topic topic
                                              :written-at (System/currentTimeMillis)})
              max-lines (long (or (:dlq/index-max-lines opts) 5000))
              append! (fn [k]
                        (let [existing (or (storage/storage-get storage k opts) "")
                              combined (str existing (when-not (empty? existing) "\n") idx-line)
                              ;; naive truncation: keep last N lines
                              lines (str/split-lines combined)
                              trimmed (if (> (count lines) max-lines)
                                        (str/join "\n" (take-last max-lines lines))
                                        combined)]
                          (storage/storage-put storage k trimmed opts)))]
          (append! index-key)
          (append! index-all-key))
        {:ok true :sink :storage :dlq-id dlq-id :path by-id-key :paths {:by-id by-id-key :by-topic by-topic-key}}
        (catch Exception e
          (logger/log logger :error ::storage-dlq-failed {:error (.getMessage e) :trace trace})
          {:ok false :error (.getMessage e)})))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter/storage
  [_ {:keys [storage logger]}]
  (->StorageDeadLetter storage logger))

