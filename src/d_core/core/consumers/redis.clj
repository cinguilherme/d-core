(ns d-core.core.consumers.redis
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [duct.logger :as logger]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.schema :as schema]
            [d-core.core.messaging.dead-letter.metadata :as dlmeta]
            [d-core.core.messaging.dead-letter :as dl]))

(defn- ensure-consumer-group!
  [conn stream group]
  (try
    (car/wcar conn
      ;; Create group starting at 0, create stream if missing.
      (car/xgroup-create stream group "0" "MKSTREAM"))
    (catch Exception _e
      ;; Ignore BUSYGROUP and similar startup races.
      nil)))

(defn- ack!
  [conn stream group redis-id]
  (car/wcar conn (car/xack stream group redis-id)))

(defn- resolve-client
  [clients client-key]
  (cond
    (map? clients) (or (get clients client-key) (get clients :default))
    :else clients))

(defn- dlq-ctx
  [{:keys [topic subscription-id stream group consumer-name redis-id dl-cfg raw-payload status client-key]}]
  {:topic topic
   :subscription-id subscription-id
   :runtime :redis
   :source {:stream stream
            :group group
            :consumer consumer-name
            :redis-id redis-id
            :client client-key}
   :producer client-key
   :deadletter dl-cfg
   :raw-payload raw-payload
   :status status})

(defn- enrich-dlq
  [envelope ctx]
  (dlmeta/enrich-for-deadletter (or envelope {:msg nil}) (dlq-ctx ctx)))

(defn- send-poison!
  "Terminal failure: DLQ (status :poison) then ACK."
  [{:keys [conn stream group dead-letter logger] :as ctx}
   {:keys [failure-type envelope exception redis-id raw-payload]}]
  (logger/log logger :warn ::redis-poison-message
              {:subscription-id (:subscription-id ctx)
               :redis-id redis-id
               :failure/type failure-type})
  (let [dlq-envelope (enrich-dlq envelope (assoc ctx
                                                :redis-id redis-id
                                                :raw-payload raw-payload
                                                :status :poison))]
    (if dead-letter
      (let [dl-res (dl/send-dead-letter! dead-letter dlq-envelope
                                         {:error exception
                                          :failure/type failure-type
                                          :retriable? false
                                          :stacktrace (with-out-str (.printStackTrace ^Throwable exception))}
                                         {})]
        (when-not (:ok dl-res)
          (logger/log logger :error ::redis-dead-letter-failed
                      {:subscription-id (:subscription-id ctx)
                       :redis-id redis-id
                       :error (:error dl-res)})))
      (logger/log logger :warn ::no-dlq-configured
                  {:subscription-id (:subscription-id ctx) :redis-id redis-id})))
  (ack! conn stream group redis-id))

(defn- validate-subscription!
  [{:keys [subscription-id subscription-schema]} envelope]
  (let [scfg (or subscription-schema {})
        view (:schema scfg)
        strictness (:strictness scfg)]
    (when view
      (schema/validate! view
                       (or (:msg envelope) envelope)
                       {:schema-id subscription-id
                        :strictness strictness})))
  envelope)

(defn- send-handler-failure!
  [{:keys [conn stream group dead-letter logger] :as ctx}
   {:keys [envelope exception redis-id raw-payload]}]
  (logger/log logger :error ::redis-handler-failed
              {:subscription-id (:subscription-id ctx)
               :redis-id redis-id
               :error (.getMessage ^Throwable exception)})
  (if dead-letter
    (let [dlq-envelope (enrich-dlq envelope (assoc ctx
                                                  :redis-id redis-id
                                                  :raw-payload raw-payload
                                                  :status nil))
          dl-res (dl/send-dead-letter! dead-letter dlq-envelope
                                       {:error exception
                                        :stacktrace (with-out-str (.printStackTrace ^Throwable exception))}
                                       {})]
      (if (:ok dl-res)
        (do
          (logger/log logger :info ::redis-dead-letter-success
                      {:subscription-id (:subscription-id ctx) :redis-id redis-id})
          (ack! conn stream group redis-id))
        (logger/log logger :error ::redis-dead-letter-failed
                    {:subscription-id (:subscription-id ctx)
                     :redis-id redis-id
                     :error (:error dl-res)})))
    ;; If no DLQ configured, we don't XACK, so it stays in PEL
    (logger/log logger :warn ::no-dlq-configured
                {:subscription-id (:subscription-id ctx) :redis-id redis-id})))

(defn- process-entry!
  [{:keys [conn stream group codec handler] :as ctx}
   redis-id fields]
  (let [m (apply hash-map fields)
        payload (get m "payload")]
    (try
      (let [envelope (try
                       (codec/decode codec payload)
                       (catch Exception e
                         (send-poison! ctx {:failure-type :codec-decode-failed
                                           :envelope nil
                                           :exception e
                                           :redis-id redis-id
                                           :raw-payload payload})
                         ::poison))]
        (when-not (= envelope ::poison)
          (let [valid?
                (try
                  (validate-subscription! ctx envelope)
                  true
                  (catch clojure.lang.ExceptionInfo e
                    (if (= :schema-invalid (:failure/type (ex-data e)))
                      (do
                        (send-poison! ctx {:failure-type :schema-invalid
                                          :envelope envelope
                                          :exception e
                                          :redis-id redis-id
                                          :raw-payload payload})
                        false)
                      (throw e))))]
            (when valid?
              (try
                (handler envelope)
                (ack! conn stream group redis-id)
                (catch Exception e
                  (send-handler-failure! ctx {:envelope envelope
                                              :exception e
                                              :redis-id redis-id
                                              :raw-payload payload})))))))
      (catch Exception e
        (logger/log (:logger ctx) :error ::redis-loop-failed
                    {:subscription-id (:subscription-id ctx)
                     :redis-id redis-id
                     :error (.getMessage ^Throwable e)})))))

(defn- start-redis-subscription!
  [{:keys [subscription-id conn stream group consumer-name codec handler dead-letter stop? block-ms logger topic routing subscription-schema client-key]}]
  (future
    (logger/log logger :report ::redis-subscription-started
                {:id subscription-id :stream stream :group group :consumer consumer-name})
    (ensure-consumer-group! conn stream group)
    (let [ctx {:subscription-id subscription-id
               :conn conn
               :stream stream
               :group group
               :consumer-name consumer-name
               :codec codec
               :handler handler
               :dead-letter dead-letter
               :logger logger
               :topic topic
               :routing routing
               :dl-cfg (routing/deadletter-config routing topic subscription-id)
               :subscription-schema subscription-schema
               :client-key client-key}]
      (while (not @stop?)
        (let [resp (car/wcar conn
                     ;; BLOCK for up to block-ms. COUNT 1 for now.
                     (car/xreadgroup "GROUP" group consumer-name
                                     "BLOCK" (str block-ms)
                                     "COUNT" "1"
                                     "STREAMS" stream ">"))]
          (doseq [[_stream entries] resp
                  [redis-id fields] entries]
            (process-entry! ctx redis-id fields))))
      (logger/log logger :report ::redis-subscription-stopped {:id subscription-id}))))

(defmethod ig/init-key :d-core.core.consumers.redis/runtime
  [_ {:keys [redis routing codec dead-letter logger]
      :or {}}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        redis-subs (into {}
                         (filter (fn [[_id sub]] (= :redis (:source sub))))
                         subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options schema client producer]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)
                           client-key (or client
                                          producer
                                          (when (and (map? redis) (contains? redis :default)) :default))
                           redis-client (resolve-client redis client-key)
                           _ (when-not redis-client
                               (throw (ex-info "Redis subscription client not configured"
                                               {:subscription-id subscription-id
                                                :client client-key
                                                :known (when (map? redis) (keys redis))})))
                           topic-cfg (routing/topic-config routing topic)
                           stream (or (:stream topic-cfg) (str "core:" (name topic)))
                           group (or (:group topic-cfg) "core")
                           consumer-name (or (:consumer options) (str "d-core-" (java.util.UUID/randomUUID)))
                           block-ms (or (:block-ms options) 5000)]
                       [subscription-id
                        (start-redis-subscription! {:subscription-id subscription-id
                                                    :conn (:conn redis-client)
                                                    :stream stream
                                                    :group group
                                                    :consumer-name consumer-name
                                                    :codec codec
                                                    :handler handler
                                                    :dead-letter dead-letter
                                                    :stop? stop?
                                                    :block-ms block-ms
                                                    :logger logger
                                                    :topic topic
                                                    :routing routing
                                                    :client-key client-key
                                                    :subscription-schema schema})])))
              redis-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :d-core.core.consumers.redis/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-redis-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)
