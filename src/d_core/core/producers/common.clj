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
            ;; Backwards-compat: allow explicit :producer/:source override.
            producer-keys (cond
                            (contains? options :producer) [(:producer options)]
                            (contains? options :source) [(:source options)]
                            (contains? options :sources) (vec (:sources options))
                            :else (routing/sources-for-topic routing topic))
            produce-one (fn [producer-key]
                          (logger/log logger :info "Producing message with producer:" producer-key)
                          (let [delegate (get producers producer-key)]
                            (when-not delegate
                              (throw (ex-info "Unknown producer key"
                                              {:producer producer-key
                                               :known (keys producers)})))
                            (logger/log logger :info ::delegating-production {:topic topic :to producer-key})
                            (tracing/with-ctx ctx
                              (p/produce! delegate msg-map (assoc options :trace trace :source producer-key)))))]
        (if (= 1 (count producer-keys))
          (produce-one (first producer-keys))
          (mapv produce-one producer-keys))))))

(defmethod ig/init-key :d-core.core.producers.common/producer
  [_ {:keys [default-producer producers routing logger]
      :or {default-producer :in-memory}}]
  (->CommonProducer default-producer producers routing logger))
