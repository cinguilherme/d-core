(ns d-core.core.producers.kafka
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.clients.kafka.client :as kc]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.producers.protocol :as p]))

(defn- topic->kafka-topic
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:kafka-topic cfg)
        (str "core." (name topic)))))

(defrecord KafkaProducer [kafka-client routing codec logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          trace (:trace options)
          kafka-topic (or (:kafka-topic options)
                          (topic->kafka-topic routing topic))
          envelope {:msg msg-map
                    :options options
                    :metadata (cond-> {}
                                trace (assoc :trace trace))
                    :produced-at (System/currentTimeMillis)}
          payload (codec/encode codec envelope)
          bytes (cond
                  (bytes? payload) payload
                  (string? payload) (.getBytes ^String payload "UTF-8")
                  :else (.getBytes (pr-str payload) "UTF-8"))
          headers (:headers options)]
      (logger/log logger :info ::producing-message {:topic topic :kafka-topic kafka-topic :trace trace})
      (let [md (kc/send! kafka-client {:topic kafka-topic
                                       :value bytes
                                       :headers headers})]
        {:ok true
         :backend :kafka
         :topic topic
         :kafka-topic kafka-topic
         :partition (:partition md)
         :offset (:offset md)
         :timestamp (:timestamp md)}))))

(defmethod ig/init-key :d-core.core.producers.kafka/producer
  [_ {:keys [kafka routing codec logger]}]
  (->KafkaProducer kafka routing codec logger))
