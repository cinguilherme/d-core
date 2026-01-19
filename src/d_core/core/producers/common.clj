(ns d-core.core.producers.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.tracing :as tracing]
            [d-core.core.messaging.routing :as routing]
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

(defrecord CommonProducer [default-producer-key producers routing logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
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
            produce-one (fn [target]
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
                                (p/produce! delegate msg-map target-opts)))))]
        (if (= 1 (count targets))
          (produce-one (first targets))
          (mapv produce-one targets))))))

(defmethod ig/init-key :d-core.core.producers.common/producer
  [_ {:keys [default-producer producers routing logger]
      :or {default-producer :in-memory}}]
  (->CommonProducer default-producer producers routing logger))
