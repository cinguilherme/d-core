(ns d-core.core.consumers.kafka
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.clients.kafka.client :as kc]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.schema :as schema]
            [d-core.core.messaging.dead-letter.metadata :as dlmeta]
            [d-core.core.messaging.dead-letter :as dl]))

(defn- topic->kafka-topic
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:kafka-topic cfg)
        (str "core." (name topic)))))

(defn- topic->group-id
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:group cfg) "core")))

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

(defn- enrich-kafka-envelope
  [{:keys [topic subscription-id kafka-topic group-id] :as ctx} r envelope raw status]
  (let [dl-cfg (routing/deadletter-config (:routing ctx) topic)]
    (dlmeta/enrich-for-deadletter
      (or envelope {:msg nil})
      {:topic topic
       :subscription-id subscription-id
       :runtime :kafka
       :source {:kafka-topic kafka-topic
                :group-id group-id
                :partition (:partition r)
                :offset (:offset r)
                :timestamp (:timestamp r)}
       :deadletter dl-cfg
       :raw-payload raw
       :status status})))

(defn- poison!
  [{:keys [dead-letter logger] :as ctx} consumer r failure-type envelope-or-nil ^Exception e raw]
  (logger/log logger :warn ::kafka-poison-message
              {:subscription-id (:subscription-id ctx)
               :topic (:topic ctx)
               :kafka-topic (:kafka-topic ctx)
               :failure/type failure-type})
  (let [dlq-envelope (enrich-kafka-envelope ctx r envelope-or-nil raw :poison)]
    (when dead-letter
      (dl/send-dead-letter! dead-letter dlq-envelope
                            {:error e
                             :failure/type failure-type
                             :retriable? false
                             :stacktrace (with-out-str (.printStackTrace e))}
                            {})))
  ;; Poison is terminal: commit offset so we don't see it again.
  (kc/commit! consumer))

(defn- handler-failure!
  [{:keys [dead-letter logger] :as ctx} consumer r envelope ^Exception e raw]
  (logger/log logger :error ::kafka-handler-failed
              {:subscription-id (:subscription-id ctx)
               :topic (:topic ctx)
               :kafka-topic (:kafka-topic ctx)
               :error (.getMessage e)})
  (if dead-letter
    (let [dlq-envelope (enrich-kafka-envelope ctx r envelope raw nil)
          dl-res (dl/send-dead-letter! dead-letter dlq-envelope
                                       {:error e
                                        :stacktrace (with-out-str (.printStackTrace e))}
                                       {})]
      (if (:ok dl-res)
        (do
          (logger/log logger :info ::kafka-dead-letter-success {:subscription-id (:subscription-id ctx)})
          (kc/commit! consumer))
        (logger/log logger :error ::kafka-dead-letter-failed
                    {:subscription-id (:subscription-id ctx) :error (:error dl-res)})))
    (logger/log logger :warn ::no-dlq-configured {:subscription-id (:subscription-id ctx)})))

(defn- process-record!
  [{:keys [codec] :as ctx} consumer r]
  (let [raw (:value r)]
    (try
      (let [envelope (try
                       (codec/decode codec raw)
                       (catch Exception e
                         (poison! ctx consumer r :codec-decode-failed nil e raw)
                         ::poison))]
        (when-not (= envelope ::poison)
          (let [valid?
                (try
                  (validate-subscription! ctx envelope)
                  true
                  (catch clojure.lang.ExceptionInfo e
                    (if (= :schema-invalid (:failure/type (ex-data e)))
                      (do (poison! ctx consumer r :schema-invalid envelope e raw) false)
                      (throw e))))]
            (when valid?
              (try
                ((:handler ctx) envelope)
                (kc/commit! consumer)
                (catch Exception e
                  (handler-failure! ctx consumer r envelope e raw)))))))
      (catch Exception e
        (logger/log (:logger ctx) :error ::kafka-loop-failed
                    {:subscription-id (:subscription-id ctx)
                     :topic (:topic ctx)
                     :kafka-topic (:kafka-topic ctx)
                     :error (.getMessage e)})))))

(defn- start-kafka-subscription!
  [{:keys [subscription-id kafka routing codec handler dead-letter stop? logger topic options subscription-schema]}]
  (future
    (let [options (or options {})
          topic (or topic :default)
          kafka-topic (or (:kafka-topic options)
                          (topic->kafka-topic routing topic))
          group-id (or (:group-id options)
                       (:group options)
                       (topic->group-id routing topic))
          poll-ms (or (:poll-ms options) 250)
          consumer (kc/make-consumer kafka {:group-id (str group-id)})]
      (logger/log logger :report ::kafka-subscription-started
                  {:id subscription-id :topic topic :kafka-topic kafka-topic :group-id group-id})
      (try
        (kc/subscribe! consumer [kafka-topic])
        ;; Join group / initial poll
        (kc/poll! consumer {:timeout-ms 100})
        (let [ctx {:subscription-id subscription-id
                   :kafka kafka
                   :routing routing
                   :codec codec
                   :handler handler
                   :dead-letter dead-letter
                   :logger logger
                   :topic topic
                   :kafka-topic kafka-topic
                   :group-id group-id
                   :subscription-schema subscription-schema}]
          (while (not @stop?)
            (doseq [r (kc/poll! consumer {:timeout-ms poll-ms})]
              (process-record! ctx consumer r))))
        (finally
          (kc/close-consumer! consumer)
          (logger/log logger :report ::kafka-subscription-stopped {:id subscription-id}))))))

(defmethod ig/init-key :d-core.core.consumers.kafka/runtime
  [_ {:keys [kafka routing codec dead-letter logger]}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        kafka-subs (into {}
                         (filter (fn [[_id sub]] (= :kafka (:source sub))))
                         subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options schema]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)]
                       [subscription-id
                        (start-kafka-subscription!
                          {:subscription-id subscription-id
                           :kafka kafka
                           :routing routing
                           :codec codec
                           :handler handler
                           :dead-letter dead-letter
                           :stop? stop?
                           :logger logger
                           :topic topic
                           :options options
                           :subscription-schema schema})])))
              kafka-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :d-core.core.consumers.kafka/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-kafka-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)

