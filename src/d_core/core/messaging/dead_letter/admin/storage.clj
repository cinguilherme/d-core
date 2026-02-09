(ns d-core.core.messaging.dead-letter.admin.storage
  (:require [integrant.core :as ig]
            [cheshire.core :as json]
            [clojure.string :as str]
            [duct.logger :as logger]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.producers.protocol :as producer]
            [d-core.core.messaging.dead-letter.admin.protocol :as admin]))

(defn- by-id-key [dlq-id]
  (str "dead-letters/by-id/" dlq-id ".json"))

(defn- index-key-for-topic [topic]
  (let [topic-name (cond
                     (keyword? topic) (name topic)
                     (string? topic) topic
                     :else (str topic))]
    (str "dead-letters/index/by-topic/" topic-name ".jsonl")))

(defn- parse-json [s]
  (when (and s (not (str/blank? s)))
    (json/parse-string s true)))

(defn- parse-jsonl [s]
  (->> (str/split-lines (or s ""))
       (remove str/blank?)
       (mapv parse-json)
       (remove nil?)
       vec))

(defrecord StorageDeadLetterAdmin [storage producer logger]
  admin/DeadLetterAdminProtocol
  (get-deadletter [_ dlq-id opts]
    (try
      (let [s (:value (storage/storage-get storage (by-id-key dlq-id) opts))
            v (parse-json s)]
        (if v
          {:ok true :item v}
          {:ok false :error :not-found :dlq-id dlq-id}))
      (catch Exception e
        (logger/log logger :error ::dlq-admin-get-failed {:dlq-id dlq-id :error (.getMessage e)})
        {:ok false :error (.getMessage e)})))

  (list-deadletters [this {:keys [topic status limit] :or {limit 50}} opts]
    (try
      (when-not topic
        (throw (ex-info "list-deadletters requires :topic (storage index is topic-scoped)" {})))
      (let [idx (:value (storage/storage-get storage (index-key-for-topic topic) opts))
            entries (parse-jsonl idx)
            ids (->> entries
                     (filter (fn [e] (or (nil? status) (= status (:status e)))))
                     (take-last (long limit))
                     (mapv :dlq-id))
            items (->> ids
                       (map (fn [id] (admin/get-deadletter this id opts)))
                       (filter :ok)
                       (mapv :item))]
        {:ok true
         :topic topic
         :count (count items)
         :items items})
      (catch Exception e
        (logger/log logger :error ::dlq-admin-list-failed {:topic topic :error (.getMessage e)})
        {:ok false :error (.getMessage e)})))

  (mark-deadletter! [this dlq-id status opts]
    (try
      (let [res (admin/get-deadletter this dlq-id opts)]
        (if-not (:ok res)
          res
          (let [item (:item res)
                topic (get-in item [:dlq :topic])
                item' (-> item
                          (assoc-in [:dlq :status] status)
                          (assoc-in [:envelope :metadata :dlq :status] status))
                payload (json/generate-string item')
                _ (storage/storage-put storage (by-id-key dlq-id) payload opts)
                ;; also refresh the topic-scoped copy, if we can infer the key
                topic-name (cond
                             (keyword? topic) (name topic)
                             (string? topic) topic
                             :else (str topic))
                by-topic-key (str "dead-letters/by-topic/" topic-name "/" dlq-id ".json")
                _ (storage/storage-put storage by-topic-key payload opts)]
            {:ok true :dlq-id dlq-id :status status})))
      (catch Exception e
        (logger/log logger :error ::dlq-admin-mark-failed {:dlq-id dlq-id :error (.getMessage e)})
        {:ok false :error (.getMessage e)})))

  (replay-deadletter! [this dlq-id opts]
    (try
      (let [res (admin/get-deadletter this dlq-id opts)]
        (if-not (:ok res)
          res
          (let [item (:item res)
                envelope (:envelope item)
                topic (or (:topic opts) (get-in envelope [:metadata :dlq :topic]) :default)
                producer-key (or (:producer opts)
                                 (:client opts)
                                 (get-in envelope [:metadata :dlq :producer]))
                msg (:msg envelope)
                ack (producer/produce! producer msg (cond-> {:topic topic
                                                             :dlq/id dlq-id
                                                             :dlq/payload-hash (get-in envelope [:metadata :dlq :payload-hash])}
                                                     producer-key (assoc :producer producer-key)))]
            {:ok true :dlq-id dlq-id :topic topic :ack ack})))
      (catch Exception e
        (logger/log logger :error ::dlq-admin-replay-failed {:dlq-id dlq-id :error (.getMessage e)})
        {:ok false :error (.getMessage e)}))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter.admin/storage
  [_ {:keys [storage producer logger]}]
  (->StorageDeadLetterAdmin storage producer logger))
