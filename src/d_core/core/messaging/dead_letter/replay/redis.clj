(ns d-core.core.messaging.dead-letter.replay.redis
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [duct.logger :as logger]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.messaging.dead-letter.metadata :as dlmeta]
            [d-core.core.messaging.dead-letter.policy :as policy]
            [d-core.core.messaging.dead-letter.defaults :as defaults]
            [d-core.core.messaging.dead-letter.admin.protocol :as dl-admin])
  (:import (java.util UUID)
           (java.util.concurrent TimeUnit)))

(defn- ensure-consumer-group!
  [conn stream group]
  (try
    (car/wcar conn
      (car/xgroup-create stream group "0" "MKSTREAM"))
    (catch Exception _e
      nil)))

(defn- topic->stream
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:stream cfg)
        (str "core:" (name topic)))))

(defn- dlq-stream
  [routing topic subscription-id]
  (let [base (topic->stream routing topic)
        dl-cfg (routing/deadletter-config routing topic subscription-id)
        suffix (or (:suffix dl-cfg) ".dl")]
    (str base suffix)))

(defn- redis-topics
  "Topics in routing subscriptions that should have redis DLQ streams monitored."
  [routing]
  (->> (get routing :subscriptions {})
       (filter (fn [[_id sub]] (= :redis (:source sub))))
       (map (fn [[subscription-id sub]]
              {:topic (or (:topic sub) :default)
               :subscription-id subscription-id}))
       vec))

(defn- decode-dlq-entry
  "Given a redis stream entry fields map, decode envelope (expects field \"payload\")."
  [codec fields]
  (let [payload (get fields "payload")]
    {:raw payload
     :envelope (codec/decode codec payload)}))

(defn- extract-original-envelope
  "DLQ producer sink publishes a payload with :original-envelope. If present, use it."
  [dlq-envelope]
  (let [m (:msg dlq-envelope)]
    (or (:original-envelope m)
        dlq-envelope)))

(defn- inc-attempt
  [envelope]
  (update-in envelope [:metadata :dlq :attempt] (fnil inc 0)))

(defn- effective-max-attempts
  [envelope default-max]
  (long (or (get-in envelope [:metadata :dlq :max-attempts])
            (get-in envelope [:metadata :dlq :deadletter :max-attempts])
            default-max)))

(defn- replay-once!
  [{:keys [conn routing codec policy admin logger group consumer block-ms count]}]
  (let [topics (redis-topics routing)
        streams (->> topics
                     (map (fn [{:keys [topic subscription-id]}]
                            (dlq-stream routing topic subscription-id)))
                     distinct
                     vec)
        _ (doseq [s streams] (ensure-consumer-group! conn s group))]
    ;; NOTE: Carmine's command macros (e.g. xreadgroup) are not reliably invokable via `apply`.
    ;; To stay safe, read one stream at a time.
    (doseq [dlq-stream streams]
      (let [resp (car/wcar conn
                   (car/xreadgroup "GROUP" group consumer
                                   "BLOCK" (str block-ms)
                                   "COUNT" (str count)
                                   "STREAMS" dlq-stream ">"))]
        ;; resp shape: [[stream [[id [field value ...]] ...]]]
        (doseq [[stream entries] resp
                [id fields] entries]
          (let [m (apply hash-map fields)
                {:keys [envelope]} (decode-dlq-entry codec m)
                dlq-id (or (get-in envelope [:msg :dlq-id])
                           (get-in envelope [:metadata :dlq :id]))
                original (extract-original-envelope envelope)
                topic (or (get-in original [:metadata :dlq :topic]) :default)
                subscription-id (get-in original [:metadata :dlq :subscription-id])
                dl-cfg (routing/deadletter-config routing topic subscription-id)
                original (dlmeta/enrich-for-deadletter original
                                                      {:topic topic
                                                       :runtime :redis-replay
                                                       :source {:dlq-stream stream
                                                                :redis-id id}
                                                       :deadletter dl-cfg})
                original (inc-attempt original)
                decision (when policy (policy/classify policy original {:error :replay} {}))
                status (or (:status decision) (get-in original [:metadata :dlq :status]) :eligible)
                original (cond-> original
                           (:max-attempts decision) (assoc-in [:metadata :dlq :max-attempts] (:max-attempts decision))
                           (:delay-ms decision) (assoc-in [:metadata :dlq :delay-ms] (:delay-ms decision))
                           true (assoc-in [:metadata :dlq :status] status))
                attempt (long (get-in original [:metadata :dlq :attempt] 0))
                max-attempts (effective-max-attempts original defaults/*default-max-attempts*)
                delay-ms (long (or (get-in original [:metadata :dlq :deadletter :delay-ms])
                                   (get-in original [:metadata :dlq :delay-ms])
                                   0))
                delay-ms (max 0 delay-ms)]
            (try
              (if (not= status :eligible)
                (do
                  (when (and admin dlq-id)
                    (dl-admin/mark-deadletter! admin dlq-id status {}))
                  (logger/log logger :warn ::dlq-not-replaying
                              {:topic topic :dlq-id dlq-id :status status :attempt attempt :max-attempts max-attempts})
                  (car/wcar conn (car/xack stream group id)))
                (let [target-stream (topic->stream routing topic)
                      payload (codec/encode codec original)]
                  (when (and (= defaults/*default-replay-mode* :sleep) (pos? delay-ms))
                    (Thread/sleep delay-ms))
                  (car/wcar conn (car/xadd target-stream "*" "payload" payload))
                  (logger/log logger :info ::dlq-replayed {:topic topic :dlq-id dlq-id :stream target-stream :attempt attempt})
                  (car/wcar conn (car/xack stream group id))))
              (catch Exception e
                (logger/log logger :error ::dlq-replay-failed {:topic topic :dlq-id dlq-id :redis-id id :error (.getMessage e)})))))))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter.replay/redis
  [_ {:keys [redis routing codec policy logger
             admin group consumer block-ms count poll-interval-ms]
      :or {group "dlq"
           consumer (str "dlq-replay-" (UUID/randomUUID))
           block-ms 1000
           count 10
           poll-interval-ms 250}}]
  (let [stop? (atom false)
        conn (:conn redis)
        ;; if policy not provided, default policy still works (no Integrant wiring required).
        policy (or policy (policy/->DefaultPolicy))]
    {:stop? stop?
     :thread
     (future
       (logger/log logger :report ::dlq-replay-started {:group group})
       (while (not @stop?)
         (replay-once! {:conn conn
                        :routing routing
                        :codec codec
                        :policy policy
                        :admin admin
                        :logger logger
                        :group group
                        :consumer consumer
                        :block-ms block-ms
                        :count count})
         (.sleep TimeUnit/MILLISECONDS (long poll-interval-ms)))
       (logger/log logger :report ::dlq-replay-stopped {:group group}))}))

(defmethod ig/halt-key! :d-core.core.messaging.dead-letter.replay/redis
  [_ {:keys [stop? thread]}]
  (when stop?
    (reset! stop? true))
  (when thread
    (deref thread 1000 nil))
  nil)
