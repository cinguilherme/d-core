(ns d-core.core.producers.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.tracing :as tracing]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.messaging.deferred.protocol :as deferred]
            [d-core.core.schema :as schema]
            [d-core.core.producers.protocol :as p]))

(defn- resolve-trace-ctx
  [options]
  (or (:trace/ctx options)
      (some-> (:trace options) tracing/decode-ctx)
      tracing/*ctx*))

(defn- normalize-targets
  [targets]
  (let [targets (cond
                  (nil? targets) []
                  (sequential? targets) targets
                  :else [targets])]
    (mapv (fn [target]
            (cond
              (keyword? target) {:producer target}
              (map? target) target
              :else (throw (ex-info "Publish targets must be keywords or maps"
                                    {:target target}))))
          targets)))

(defn- target-producer-key
  [target]
  (or (:producer target) (:client target)))

(defn- produce-target!
  [{:keys [base-options ctx defer? deferred deliver-at-ms logger msg-map now-ms
           producers trace topic]} target]
  (let [producer-key (target-producer-key target)]
    (when-not producer-key
      (throw (ex-info "Publish target missing :producer/:client"
                      {:target target})))
    (logger/log logger :info "Producing message with producer:" producer-key)
    (let [delegate (get producers producer-key)
          target-opts (merge base-options
                             (dissoc target :producer :client)
                             {:producer producer-key
                              :trace trace})]
      (when-not delegate
        (throw (ex-info "Unknown producer key"
                        {:producer producer-key
                         :known (keys producers)})))
      (logger/log logger :info ::delegating-production {:topic topic :to producer-key})
      (tracing/with-ctx ctx
        (if defer?
          (if deferred
            (do
              (logger/log logger :info ::scheduling-deferred
                          {:topic topic
                           :producer producer-key
                           :deliver-at-ms deliver-at-ms})
              (deferred/schedule! deferred {:producer producer-key
                                            :msg msg-map
                                            :options target-opts
                                            :deliver-at-ms deliver-at-ms
                                            :scheduled-at-ms now-ms}))
            (throw (ex-info "Deferred delivery requested but no scheduler configured"
                            {:topic topic
                             :producer producer-key
                             :deliver-at-ms deliver-at-ms})))
          (p/produce! delegate msg-map target-opts))))))

(defn- resolve-deliver-at-ms
  [options]
  (let [deliver-at-time (:deliver-at-time options)
        deliver-at-ms (:deliver-at-ms options)
        delay-ms (:delay-ms options)]
    (cond
      (instance? java.util.Date deliver-at-time)
      (.getTime ^java.util.Date deliver-at-time)

      (some? deliver-at-time)
      (throw (ex-info "Unsupported :deliver-at-time value"
                      {:deliver-at-time deliver-at-time}))

      (number? deliver-at-ms)
      (long deliver-at-ms)

      (some? deliver-at-ms)
      (throw (ex-info "Unsupported :deliver-at-ms value"
                      {:deliver-at-ms deliver-at-ms}))

      (number? delay-ms)
      (+ (System/currentTimeMillis) (long delay-ms))

      (some? delay-ms)
      (throw (ex-info "Unsupported :delay-ms value"
                      {:delay-ms delay-ms}))

      :else nil)))

(defrecord CommonProducer [default-producer-key producers routing deferred logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          deliver-at-ms (resolve-deliver-at-ms options)
          now-ms (System/currentTimeMillis)
          defer? (and deliver-at-ms (> deliver-at-ms now-ms))
          topic-cfg (routing/topic-config routing topic)
          schema-cfg (:schema topic-cfg)
          canonical (:canonical schema-cfg)
          strictness (or (:strictness schema-cfg) :strict)
          schema-id (or (:id schema-cfg) topic)]
      ;; Enforce producer contract *before* we encode/publish.
      (when canonical
        (schema/validate! canonical msg-map {:schema-id schema-id
                                             :strictness strictness}))
      (let [parent-ctx (resolve-trace-ctx options)
            ctx (tracing/child-ctx parent-ctx)
            trace (tracing/encode-ctx ctx)
            base-options (dissoc options :producer :source :sources :target :targets)
            ;; Backwards-compat: allow explicit :producer/:source override.
            targets (cond
                      (contains? options :targets) (normalize-targets (:targets options))
                      (contains? options :target) (normalize-targets (:target options))
                      (contains? options :producer) [{:producer (:producer options)}]
                      (contains? options :source) [{:producer (:source options)}]
                      :else (normalize-targets (routing/publish-targets routing topic)))
            _ (when (empty? targets)
                (throw (ex-info "No publish targets configured"
                                {:topic topic})))
            producer-ctx {:base-options base-options
                          :ctx ctx
                          :defer? defer?
                          :deferred deferred
                          :deliver-at-ms deliver-at-ms
                          :logger logger
                          :msg-map msg-map
                          :now-ms now-ms
                          :producers producers
                          :trace trace
                          :topic topic}]
        (if (= 1 (count targets))
          (produce-target! producer-ctx (first targets))
          (mapv (partial produce-target! producer-ctx) targets))))))

(defmethod ig/init-key :d-core.core.producers.common/producer
  [_ {:keys [default-producer producers routing deferred logger]
      :or {default-producer :in-memory}}]
  (->CommonProducer default-producer producers routing deferred logger))
