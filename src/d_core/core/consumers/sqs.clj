(ns d-core.core.consumers.sqs
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.clients.sqs.client :as sc]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.schema :as schema]
            [d-core.core.messaging.dead-letter.metadata :as dlmeta]
            [d-core.core.messaging.dead-letter :as dl]))

(defn- resolve-client
  [clients client-key]
  (cond
    (and (map? clients) (not (record? clients)))
    (or (get clients client-key) (get clients :default))
    :else clients))

(defn- queue-name
  [routing topic options]
  (let [topic (or topic :default)
        topic-cfg (routing/topic-config routing topic)]
    (or (:queue options)
        (:queue topic-cfg)
        (str "core." (name topic)))))

(defn- resolve-queue-url
  [sqs routing topic queue options]
  (let [topic-cfg (routing/topic-config routing topic)
        queue-url (or (:queue-url options) (:queue-url topic-cfg))
        ensure? (if (contains? options :ensure-queue?)
                  (:ensure-queue? options)
                  (:ensure-queue? topic-cfg))
        attrs (or (:attributes options) (:attributes topic-cfg))
        create-opts (cond-> {}
                      (seq attrs) (assoc :attributes attrs))]
    (cond
      queue-url queue-url
      ensure? (sc/create-queue! sqs queue create-opts)
      :else (or (sc/queue-url sqs queue)
                (throw (ex-info "SQS queue not found"
                                {:queue queue}))))))

(defn- dlq-ctx
  [{:keys [topic subscription-id queue queue-url message-id receipt-handle raw-payload status client-key dl-cfg]}]
  {:topic topic
   :subscription-id subscription-id
   :runtime :sqs
   :source {:queue queue
            :queue-url queue-url
            :message-id message-id
            :receipt-handle receipt-handle
            :client client-key}
   :producer client-key
   :deadletter dl-cfg
   :raw-payload raw-payload
   :status status})

(defn- enrich-dlq
  [envelope ctx]
  (dlmeta/enrich-for-deadletter (or envelope {:msg nil}) (dlq-ctx ctx)))

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

(defn- send-poison!
  [{:keys [sqs queue queue-url dead-letter logger] :as ctx}
   {:keys [failure-type envelope exception message-id receipt-handle raw-payload]}]
  (logger/log logger :warn ::sqs-poison-message
              {:subscription-id (:subscription-id ctx)
               :queue queue
               :message-id message-id
               :failure/type failure-type})
  (let [dlq-envelope (enrich-dlq envelope (assoc ctx
                                                :queue queue
                                                :queue-url queue-url
                                                :message-id message-id
                                                :receipt-handle receipt-handle
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
          (logger/log logger :error ::sqs-dead-letter-failed
                      {:subscription-id (:subscription-id ctx)
                       :message-id message-id
                       :error (:error dl-res)})))
      (logger/log logger :warn ::no-dlq-configured
                  {:subscription-id (:subscription-id ctx) :message-id message-id})))
  (sc/delete-message! sqs queue-url receipt-handle))

(defn- send-handler-failure!
  [{:keys [sqs queue queue-url dead-letter logger] :as ctx}
   {:keys [envelope exception message-id receipt-handle raw-payload]}]
  (logger/log logger :error ::sqs-handler-failed
              {:subscription-id (:subscription-id ctx)
               :message-id message-id
               :error (.getMessage ^Throwable exception)})
  (if dead-letter
    (let [dlq-envelope (enrich-dlq envelope (assoc ctx
                                                  :queue queue
                                                  :queue-url queue-url
                                                  :message-id message-id
                                                  :receipt-handle receipt-handle
                                                  :raw-payload raw-payload
                                                  :status nil))
          dl-res (dl/send-dead-letter! dead-letter dlq-envelope
                                       {:error exception
                                        :stacktrace (with-out-str (.printStackTrace ^Throwable exception))}
                                       {})]
      (if (:ok dl-res)
        (do
          (logger/log logger :info ::sqs-dead-letter-success
                      {:subscription-id (:subscription-id ctx) :message-id message-id})
          (sc/delete-message! sqs queue-url receipt-handle))
        (logger/log logger :error ::sqs-dead-letter-failed
                    {:subscription-id (:subscription-id ctx)
                     :message-id message-id
                     :error (:error dl-res)})))
    (logger/log logger :warn ::no-dlq-configured
                {:subscription-id (:subscription-id ctx) :message-id message-id})))

(defn- process-message!
  [{:keys [codec handler] :as ctx}
   {:keys [message-id receipt-handle body] :as msg}]
  (try
    (let [envelope (try
                     (codec/decode codec body)
                     (catch Exception e
                       (send-poison! ctx {:failure-type :codec-decode-failed
                                          :envelope nil
                                          :exception e
                                          :message-id message-id
                                          :receipt-handle receipt-handle
                                          :raw-payload body})
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
                                         :message-id message-id
                                         :receipt-handle receipt-handle
                                         :raw-payload body})
                      false)
                    (throw e))))]
          (when valid?
            (try
              (handler envelope)
              (sc/delete-message! (:sqs ctx) (:queue-url ctx) receipt-handle)
              (catch Exception e
                (send-handler-failure! ctx {:envelope envelope
                                            :exception e
                                            :message-id message-id
                                            :receipt-handle receipt-handle
                                            :raw-payload body})))))))
    (catch Exception e
      (logger/log (:logger ctx) :error ::sqs-loop-failed
                  {:subscription-id (:subscription-id ctx)
                   :message-id message-id
                   :error (.getMessage ^Throwable e)}))))

(defn- start-sqs-subscription!
  [{:keys [subscription-id sqs routing codec handler dead-letter stop? logger topic options subscription-schema client-key]}]
  (future
    (let [options (or options {})
          topic (or topic :default)
          queue (queue-name routing topic options)
          queue-url (resolve-queue-url sqs routing topic queue options)
          wait-seconds (or (:wait-seconds options) 10)
          max-messages (or (:max-messages options) 1)
          visibility-timeout (:visibility-timeout options)
          idle-sleep-ms (or (:idle-sleep-ms options) 200)]
      (logger/log logger :report ::sqs-subscription-started
                  {:id subscription-id :topic topic :queue queue :queue-url queue-url})
      (let [ctx {:subscription-id subscription-id
                 :sqs sqs
                 :routing routing
                 :codec codec
                 :handler handler
                 :dead-letter dead-letter
                 :logger logger
                 :topic topic
                 :queue queue
                 :queue-url queue-url
                 :dl-cfg (routing/deadletter-config routing topic subscription-id)
                 :subscription-schema subscription-schema
                 :client-key client-key}]
        (while (not @stop?)
          (try
            (let [messages (sc/receive-messages! sqs queue-url
                                                {:max-messages max-messages
                                                 :wait-seconds wait-seconds
                                                 :visibility-timeout visibility-timeout})]
              (if (seq messages)
                (doseq [msg messages]
                  (process-message! ctx msg))
                (Thread/sleep idle-sleep-ms)))
            (catch Exception e
              (logger/log logger :error ::sqs-subscription-failed
                          {:id subscription-id :error (.getMessage ^Throwable e)}))))
        (logger/log logger :report ::sqs-subscription-stopped {:id subscription-id})))))

(defmethod ig/init-key :d-core.core.consumers.sqs/runtime
  [_ {:keys [sqs routing codec dead-letter logger]}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        sqs-subs (into {}
                       (filter (fn [[_id sub]] (= :sqs (:source sub))))
                       subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options schema client producer]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)
                           client-key (or client
                                          producer
                                          (when (and (map? sqs) (contains? sqs :default)) :default))
                           sqs-client (resolve-client sqs client-key)]
                       (when-not sqs-client
                         (throw (ex-info "SQS subscription client not configured"
                                         {:subscription-id subscription-id
                                          :client client-key
                                          :known (when (map? sqs) (keys sqs))})))
                       [subscription-id
                        (start-sqs-subscription!
                          {:subscription-id subscription-id
                           :sqs sqs-client
                           :routing routing
                           :codec codec
                           :handler handler
                           :dead-letter dead-letter
                           :stop? stop?
                           :logger logger
                           :topic topic
                           :options options
                           :client-key client-key
                           :subscription-schema schema})])))
              sqs-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :d-core.core.consumers.sqs/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-sqs-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)
