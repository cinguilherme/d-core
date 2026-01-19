(ns d-core.core.consumers.rabbitmq
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.clients.rabbitmq.client :as rc]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.schema :as schema]
            [d-core.core.messaging.dead-letter.metadata :as dlmeta]
            [d-core.core.messaging.dead-letter :as dl])
  (:import (com.rabbitmq.client DefaultConsumer Envelope)))

(defn- topic->queue
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:queue cfg)
        (str "core." (name topic)))))

(defn- topic->exchange
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:exchange cfg) "")))

(defn- topic->routing-key
  [routing topic queue]
  (let [cfg (routing/topic-config routing topic)]
    (or (:routing-key cfg) queue)))

(defn- ensure-topology!
  [channel {:keys [exchange exchange-type queue routing-key durable? exclusive? auto-delete?]}]
  (when (and exchange (not (empty? exchange)))
    (.exchangeDeclare channel exchange (or exchange-type "direct")
                      (boolean durable?)
                      (boolean auto-delete?)
                      nil))
  (when queue
    (.queueDeclare channel queue
                   (boolean durable?)
                   (boolean exclusive?)
                   (boolean auto-delete?)
                   nil)
    (when (and exchange (not (empty? exchange)) routing-key)
      (.queueBind channel queue exchange routing-key))))

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

(defn- dlq-ctx
  [{:keys [topic subscription-id exchange queue routing-key delivery-tag redelivered? dl-cfg raw-payload status]}]
  {:topic topic
   :subscription-id subscription-id
   :runtime :rabbitmq
   :source {:exchange exchange
            :queue queue
            :routing-key routing-key
            :delivery-tag delivery-tag
            :redelivered redelivered?}
   :deadletter dl-cfg
   :raw-payload raw-payload
   :status status})

(defn- enrich-dlq
  [envelope ctx]
  (dlmeta/enrich-for-deadletter (or envelope {:msg nil}) (dlq-ctx ctx)))

(defn- ack!
  [channel delivery-tag]
  (.basicAck channel delivery-tag false))

(defn- send-poison!
  [{:keys [channel dead-letter logger] :as ctx}
   {:keys [failure-type envelope exception delivery-tag raw-payload redelivered?]}]
  (logger/log logger :warn ::rabbitmq-poison-message
              {:subscription-id (:subscription-id ctx)
               :queue (:queue ctx)
               :failure/type failure-type})
  (let [dlq-envelope (enrich-dlq envelope (assoc ctx
                                                :delivery-tag delivery-tag
                                                :raw-payload raw-payload
                                                :redelivered? redelivered?
                                                :status :poison))]
    (if dead-letter
      (let [dl-res (dl/send-dead-letter! dead-letter dlq-envelope
                                         {:error exception
                                          :failure/type failure-type
                                          :retriable? false
                                          :stacktrace (with-out-str (.printStackTrace ^Throwable exception))}
                                         {})]
        (when-not (:ok dl-res)
          (logger/log logger :error ::rabbitmq-dead-letter-failed
                      {:subscription-id (:subscription-id ctx)
                       :delivery-tag delivery-tag
                       :error (:error dl-res)})))
      (logger/log logger :warn ::no-dlq-configured
                  {:subscription-id (:subscription-id ctx) :delivery-tag delivery-tag})))
  (ack! channel delivery-tag))

(defn- send-handler-failure!
  [{:keys [channel dead-letter logger] :as ctx}
   {:keys [envelope exception delivery-tag raw-payload redelivered?]}]
  (logger/log logger :error ::rabbitmq-handler-failed
              {:subscription-id (:subscription-id ctx)
               :delivery-tag delivery-tag
               :error (.getMessage ^Throwable exception)})
  (if dead-letter
    (let [dlq-envelope (enrich-dlq envelope (assoc ctx
                                                  :delivery-tag delivery-tag
                                                  :raw-payload raw-payload
                                                  :redelivered? redelivered?
                                                  :status nil))
          dl-res (dl/send-dead-letter! dead-letter dlq-envelope
                                       {:error exception
                                        :stacktrace (with-out-str (.printStackTrace ^Throwable exception))}
                                       {})]
      (if (:ok dl-res)
        (do
          (logger/log logger :info ::rabbitmq-dead-letter-success
                      {:subscription-id (:subscription-id ctx) :delivery-tag delivery-tag})
          (ack! channel delivery-tag))
        (logger/log logger :error ::rabbitmq-dead-letter-failed
                    {:subscription-id (:subscription-id ctx)
                     :delivery-tag delivery-tag
                     :error (:error dl-res)})))
    (logger/log logger :warn ::no-dlq-configured
                {:subscription-id (:subscription-id ctx) :delivery-tag delivery-tag})))

(defn- process-delivery!
  [{:keys [channel codec] :as ctx} ^Envelope envelope payload]
  (let [delivery-tag (.getDeliveryTag envelope)
        redelivered? (.isRedeliver envelope)]
    (try
      (let [msg-envelope (try
                           (codec/decode codec payload)
                           (catch Exception e
                             (send-poison! ctx {:failure-type :codec-decode-failed
                                               :envelope nil
                                               :exception e
                                               :delivery-tag delivery-tag
                                               :raw-payload payload
                                               :redelivered? redelivered?})
                             ::poison))]
        (when-not (= msg-envelope ::poison)
          (let [valid?
                (try
                  (validate-subscription! ctx msg-envelope)
                  true
                  (catch clojure.lang.ExceptionInfo e
                    (if (= :schema-invalid (:failure/type (ex-data e)))
                      (do
                        (send-poison! ctx {:failure-type :schema-invalid
                                          :envelope msg-envelope
                                          :exception e
                                          :delivery-tag delivery-tag
                                          :raw-payload payload
                                          :redelivered? redelivered?})
                        false)
                      (throw e))))]
            (when valid?
              (try
                ((:handler ctx) msg-envelope)
                (ack! channel delivery-tag)
                (catch Exception e
                  (send-handler-failure! ctx {:envelope msg-envelope
                                              :exception e
                                              :delivery-tag delivery-tag
                                              :raw-payload payload
                                              :redelivered? redelivered?})))))))
      (catch Exception e
        (logger/log (:logger ctx) :error ::rabbitmq-loop-failed
                    {:subscription-id (:subscription-id ctx)
                     :delivery-tag delivery-tag
                     :error (.getMessage ^Throwable e)})))))

(defn- start-rabbitmq-subscription!
  [{:keys [subscription-id rabbitmq routing codec handler dead-letter stop? logger topic options subscription-schema]}]
  (future
    (let [options (or options {})
          topic (or topic :default)
          topic-cfg (routing/topic-config routing topic)
          exchange (or (:exchange options) (:exchange topic-cfg) (topic->exchange routing topic))
          queue (or (:queue options) (:queue topic-cfg) (topic->queue routing topic))
          routing-key (or (:routing-key options) (:routing-key topic-cfg) (topic->routing-key routing topic queue))
          exchange-type (or (:exchange-type options) (:exchange-type topic-cfg) "direct")
          durable? (if (contains? options :durable?)
                     (:durable? options)
                     (if (contains? topic-cfg :durable?)
                       (:durable? topic-cfg)
                       true))
          exclusive? (if (contains? options :exclusive?)
                       (:exclusive? options)
                       (if (contains? topic-cfg :exclusive?)
                         (:exclusive? topic-cfg)
                         false))
          auto-delete? (if (contains? options :auto-delete?)
                         (:auto-delete? options)
                         (if (contains? topic-cfg :auto-delete?)
                           (:auto-delete? topic-cfg)
                           false))
          prefetch (or (:prefetch options) 1)
          channel (rc/open-channel rabbitmq)
          consumer-tag (atom nil)]
      (logger/log logger :report ::rabbitmq-subscription-started
                  {:id subscription-id :topic topic :exchange exchange :queue queue :routing-key routing-key})
      (try
        (ensure-topology! channel {:exchange exchange
                                   :exchange-type exchange-type
                                   :queue queue
                                   :routing-key routing-key
                                   :durable? durable?
                                   :exclusive? exclusive?
                                   :auto-delete? auto-delete?})
        (.basicQos channel (int prefetch))
        (let [ctx {:subscription-id subscription-id
                   :routing routing
                   :codec codec
                   :handler handler
                   :dead-letter dead-letter
                   :logger logger
                   :topic topic
                   :exchange exchange
                   :queue queue
                   :routing-key routing-key
                   :channel channel
                   :dl-cfg (routing/deadletter-config routing topic)
                   :subscription-schema subscription-schema}
              consumer (proxy [DefaultConsumer] [channel]
                         (handleDelivery [tag env props body]
                           (process-delivery! ctx env body)))]
          (reset! consumer-tag (.basicConsume channel queue false consumer))
          (while (not @stop?)
            (Thread/sleep 200)))
        (catch Exception e
          (logger/log logger :error ::rabbitmq-subscription-failed
                      {:id subscription-id :error (.getMessage ^Throwable e)}))
        (finally
          (try
            (when-let [tag @consumer-tag]
              (.basicCancel channel tag))
            (catch Exception _e
              nil))
          (try
            (.close channel)
            (catch Exception _e
              nil))
          (logger/log logger :report ::rabbitmq-subscription-stopped {:id subscription-id})))))))

(defmethod ig/init-key :d-core.core.consumers.rabbitmq/runtime
  [_ {:keys [rabbitmq routing codec dead-letter logger]}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        rabbitmq-subs (into {}
                            (filter (fn [[_id sub]] (= :rabbitmq (:source sub))))
                            subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options schema]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)]
                       [subscription-id
                        (start-rabbitmq-subscription!
                          {:subscription-id subscription-id
                           :rabbitmq rabbitmq
                           :routing routing
                           :codec codec
                           :handler handler
                           :dead-letter dead-letter
                           :stop? stop?
                           :logger logger
                           :topic topic
                           :options options
                           :subscription-schema schema})])))
              rabbitmq-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :d-core.core.consumers.rabbitmq/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-rabbitmq-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)
