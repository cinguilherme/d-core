(ns d-core.core.messaging.dead-letter.replay.redis
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [duct.logger :as logger]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.messaging.dead-letter.metadata :as dlmeta]
            [d-core.core.messaging.dead-letter.policy :as policy])
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
  [{:keys [conn routing codec policy logger retry-stream stuck-stream poison-stream manual-stream]
    :or {stuck-stream "dlq:stuck"
         poison-stream "dlq:poison"
         manual-stream "dlq:manual"}}]
  (let [resp (car/wcar conn
               (car/xreadgroup "GROUP" (:group retry-stream) (:consumer retry-stream)
                               "BLOCK" (str (:block-ms retry-stream))
                               "COUNT" (str (:count retry-stream))
                               "STREAMS" (:stream retry-stream) ">"))]
    ;; resp shape: [[stream [[id [field value ...]] ...]]]
    (doseq [[_stream entries] resp
            [id fields] entries]
      (let [m (apply hash-map fields)
            {:keys [envelope]} (decode-dlq-entry codec m)
            original (extract-original-envelope envelope)
            topic (or (get-in original [:metadata :dlq :topic]) :default)
            dl-cfg (routing/deadletter-config routing topic)
            original (dlmeta/enrich-for-deadletter original
                                                  {:topic topic
                                                   :runtime :redis-replay
                                                   :source {:retry-stream (:stream retry-stream)
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
            max-attempts (effective-max-attempts original 3)
            dest-stream (case status
                          :poison poison-stream
                          :manual manual-stream
                          :stuck stuck-stream
                          nil)]
        (try
          (if (not= status :eligible)
            (let [payload (codec/encode codec original)]
              (car/wcar conn (car/xadd dest-stream "*" "payload" payload))
              (logger/log logger :warn ::dlq-diverted
                          {:topic topic
                           :status status
                           :attempt attempt
                           :max-attempts max-attempts
                           :stream dest-stream})
              (car/wcar conn (car/xack (:stream retry-stream) (:group retry-stream) id)))
            (let [target-stream (topic->stream routing topic)
                  payload (codec/encode codec original)]
              (car/wcar conn (car/xadd target-stream "*" "payload" payload))
              (logger/log logger :info ::dlq-replayed {:topic topic :stream target-stream :attempt attempt})
              (car/wcar conn (car/xack (:stream retry-stream) (:group retry-stream) id))))
          (catch Exception e
            (logger/log logger :error ::dlq-replay-failed {:topic topic :redis-id id :error (.getMessage e)})))))))

(defmethod ig/init-key :d-core.core.messaging.dead-letter.replay/redis
  [_ {:keys [redis routing codec policy logger
             retry-stream stuck-stream poison-stream manual-stream poll-interval-ms]
      :or {retry-stream {:stream "dlq:retry"
                         :group "dlq"
                         :consumer (str "dlq-replay-" (UUID/randomUUID))
                         :block-ms 1000
                         :count 10}
           stuck-stream "dlq:stuck"
           poison-stream "dlq:poison"
           manual-stream "dlq:manual"
           poll-interval-ms 250}}]
  (let [stop? (atom false)
        conn (:conn redis)
        ;; if policy not provided, default policy still works (no Integrant wiring required).
        policy (or policy (policy/->DefaultPolicy))]
    (ensure-consumer-group! conn (:stream retry-stream) (:group retry-stream))
    {:stop? stop?
     :thread
     (future
       (logger/log logger :report ::dlq-replay-started {:retry-stream (:stream retry-stream)})
       (while (not @stop?)
         (replay-once! {:conn conn
                        :routing routing
                        :codec codec
                        :policy policy
                        :logger logger
                        :retry-stream retry-stream
                        :stuck-stream stuck-stream
                        :poison-stream poison-stream
                        :manual-stream manual-stream})
         (.sleep TimeUnit/MILLISECONDS (long poll-interval-ms)))
       (logger/log logger :report ::dlq-replay-stopped {:retry-stream (:stream retry-stream)}))}))

(defmethod ig/halt-key! :d-core.core.messaging.dead-letter.replay/redis
  [_ {:keys [stop? thread]}]
  (when stop?
    (reset! stop? true))
  (when thread
    (deref thread 1000 nil))
  nil)

