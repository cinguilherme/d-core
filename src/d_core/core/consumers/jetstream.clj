(ns d-core.core.consumers.jetstream
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.schema :as schema]
            [d-core.core.messaging.dead-letter.metadata :as dlmeta]
            [d-core.core.messaging.dead-letter :as dl])
  (:import (io.nats.client JetStream JetStreamManagement JetStreamSubscription Message)
           (io.nats.client.api StreamConfiguration StorageType ConsumerConfiguration AckPolicy DeliverPolicy)
           (io.nats.client PullSubscribeOptions)
           (java.time Duration)))

(defn- topic->subject
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:subject cfg)
        (str "core." (name topic)))))

(defn- topic->stream
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:stream cfg)
        (str "core_" (name topic)))))

(defn- topic->durable
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:durable cfg)
        (str "core_" (name topic)))))

(defn- ensure-stream!
  [^JetStreamManagement jsm stream subject]
  (try
    (.getStreamInfo jsm stream)
    (catch Exception _e
      (let [cfg (-> (StreamConfiguration/builder)
                    (.name stream)
                    (.storageType StorageType/File)
                    (.subjects (into-array String [subject]))
                    (.build))]
        (try
          (.addStream jsm cfg)
          (catch Exception _e2
            nil))))))

(defn- ensure-consumer!
  [^JetStreamManagement jsm stream durable subject]
  (let [cfg (-> (ConsumerConfiguration/builder)
                (.durable durable)
                (.filterSubject subject)
                (.ackPolicy AckPolicy/Explicit)
                (.deliverPolicy DeliverPolicy/All)
                (.build))]
    ;; idempotent-ish: updates if exists, creates if missing
    (.addOrUpdateConsumer jsm stream cfg)))

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

(defn- enrich-jetstream-envelope
  [{:keys [routing topic subscription-id subject stream durable]} envelope payload status]
  (let [dl-cfg (routing/deadletter-config routing topic)]
    (dlmeta/enrich-for-deadletter
      (or envelope {:msg nil})
      {:topic topic
       :subscription-id subscription-id
       :runtime :jetstream
       :source {:subject subject
                :stream stream
                :durable durable}
       :deadletter dl-cfg
       :raw-payload payload
       :status status})))

(defn- poison!
  [{:keys [dead-letter logger] :as ctx} ^Message msg failure-type envelope-or-nil ^Exception e payload]
  (logger/log logger :warn ::jetstream-poison-message
              {:subscription-id (:subscription-id ctx)
               :topic (:topic ctx)
               :subject (:subject ctx)
               :failure/type failure-type})
  (let [dlq-envelope (enrich-jetstream-envelope ctx envelope-or-nil payload :poison)]
    (when dead-letter
      (dl/send-dead-letter! dead-letter dlq-envelope
                            {:error e
                             :failure/type failure-type
                             :retriable? false
                             :stacktrace (with-out-str (.printStackTrace e))}
                            {})))
  ;; Poison is terminal: ACK so we don't see it again.
  (.ack msg))

(defn- handler-failure!
  [{:keys [dead-letter logger] :as ctx} ^Message msg envelope ^Exception e payload]
  (logger/log logger :error ::jetstream-handler-failed
              {:subscription-id (:subscription-id ctx)
               :topic (:topic ctx)
               :subject (:subject ctx)
               :error (.getMessage e)})
  (if dead-letter
    (let [dlq-envelope (enrich-jetstream-envelope ctx envelope payload nil)
          dl-res (dl/send-dead-letter! dead-letter dlq-envelope
                                       {:error e
                                        :stacktrace (with-out-str (.printStackTrace e))}
                                       {})]
      (if (:ok dl-res)
        (do
          (logger/log logger :info ::jetstream-dead-letter-success {:subscription-id (:subscription-id ctx)})
          (.ack msg))
        (logger/log logger :error ::jetstream-dead-letter-failed
                    {:subscription-id (:subscription-id ctx) :error (:error dl-res)})))
    (logger/log logger :warn ::no-dlq-configured {:subscription-id (:subscription-id ctx)})))

(defn- process-message!
  [{:keys [codec] :as ctx} ^Message msg]
  (let [payload (.getData msg)]
    (try
      (let [envelope (try
                       (codec/decode codec payload)
                       (catch Exception e
                         (poison! ctx msg :codec-decode-failed nil e payload)
                         ::poison))]
        (when-not (= envelope ::poison)
          (let [valid?
                (try
                  (validate-subscription! ctx envelope)
                  true
                  (catch clojure.lang.ExceptionInfo e
                    (if (= :schema-invalid (:failure/type (ex-data e)))
                      (do (poison! ctx msg :schema-invalid envelope e payload) false)
                      (throw e))))]
            (when valid?
              (try
                ((:handler ctx) envelope)
                (.ack msg)
                (catch Exception e
                  (handler-failure! ctx msg envelope e payload)))))))
      (catch Exception e
        (logger/log (:logger ctx) :error ::jetstream-loop-failed
                    {:subscription-id (:subscription-id ctx)
                     :topic (:topic ctx)
                     :subject (:subject ctx)
                     :error (.getMessage e)})))))

(defn- start-jetstream-subscription!
  [{:keys [subscription-id js jsm routing codec handler dead-letter stop? logger topic options subscription-schema]}]
  (future
    (let [options (or options {})
          subject (topic->subject routing topic)
          stream (topic->stream routing topic)
          durable (or (:durable options) (topic->durable routing topic))
          batch (or (:pull-batch options) 1)
          expires-ms (or (:expires-ms options) 1000)]
      (ensure-stream! ^JetStreamManagement jsm stream subject)
      (ensure-consumer! ^JetStreamManagement jsm stream durable subject)
      (let [pull-opts (PullSubscribeOptions/bind stream durable)
            ^JetStreamSubscription sub (.subscribe ^JetStream js subject pull-opts)]
        (logger/log logger :report ::jetstream-subscription-started
                    {:id subscription-id :topic topic :subject subject :stream stream :durable durable})
        (try
          (let [ctx {:subscription-id subscription-id
                     :routing routing
                     :codec codec
                     :handler handler
                     :dead-letter dead-letter
                     :logger logger
                     :topic topic
                     :subject subject
                     :stream stream
                     :durable durable
                     :subscription-schema subscription-schema}]
            (while (not @stop?)
              (doseq [^Message m (.fetch sub (int batch) (Duration/ofMillis (long expires-ms)))]
                (process-message! ctx m))))
          (finally
            (try (.unsubscribe sub) (catch Exception _e nil))
            (logger/log logger :report ::jetstream-subscription-stopped {:id subscription-id})))))))

(defmethod ig/init-key :d-core.core.consumers.jetstream/runtime
  [_ {:keys [jetstream routing codec dead-letter logger]}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        js-subs (into {}
                      (filter (fn [[_id sub]] (= :jetstream (:source sub))))
                      subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options schema]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)]
                       [subscription-id
                        (start-jetstream-subscription!
                          {:subscription-id subscription-id
                           :js (:js jetstream)
                           :jsm (:jsm jetstream)
                           :routing routing
                           :codec codec
                           :handler handler
                           :dead-letter dead-letter
                           :stop? stop?
                           :logger logger
                           :topic topic
                           :options options
                           :subscription-schema schema})])))
              js-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :d-core.core.consumers.jetstream/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-jetstream-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)

