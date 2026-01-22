(ns d-core.core.producers.rabbitmq
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.producers.protocol :as p]
            [d-core.core.clients.rabbitmq.client :as rc])
  (:import (com.rabbitmq.client AMQP$BasicProperties$Builder)))

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
  (when (seq exchange)
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
    (when (and (seq exchange) routing-key)
      (.queueBind channel queue exchange routing-key))))

(defn- ->properties
  [{:keys [headers content-type delivery-mode persistent?]}]
  (let [builder (AMQP$BasicProperties$Builder.)]
    (when headers
      (.headers builder (into {} (map (fn [[k v]] [(name k) v])) headers)))
    (when content-type
      (.contentType builder content-type))
    (when delivery-mode
      (.deliveryMode builder (int delivery-mode)))
    (when persistent?
      (.deliveryMode builder (int 2)))
    (.build builder)))

(defrecord RabbitMQProducer [rabbitmq routing codec logger channel channel-lock]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          trace (:trace options)
          topic-cfg (routing/topic-config routing topic)
          exchange (or (:exchange options)
                       (:exchange topic-cfg)
                       (topic->exchange routing topic))
          queue (or (:queue options)
                    (:queue topic-cfg)
                    (topic->queue routing topic))
          routing-key (or (:routing-key options)
                          (:routing-key topic-cfg)
                          (topic->routing-key routing topic queue))
          exchange-type (or (:exchange-type options)
                            (:exchange-type topic-cfg)
                            "direct")
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
          envelope {:msg msg-map
                    :options options
                    :metadata (cond-> {}
                                trace (assoc :trace trace))
                    :produced-at (System/currentTimeMillis)}
          payload (codec/encode codec envelope)
          bytes (if (bytes? payload)
                  payload
                  (.getBytes (str payload) "UTF-8"))
          props (->properties options)]
      (logger/log logger :info ::producing-message
                  {:topic topic :exchange exchange :queue queue :routing-key routing-key :trace trace})
      (locking channel-lock
        (ensure-topology! channel {:exchange exchange
                                   :exchange-type exchange-type
                                   :queue queue
                                   :routing-key routing-key
                                   :durable? durable?
                                   :exclusive? exclusive?
                                   :auto-delete? auto-delete?})
        (.basicPublish channel exchange routing-key props bytes))
      {:ok true
       :backend :rabbitmq
       :topic topic
       :exchange exchange
       :queue queue
       :routing-key routing-key})))

(defmethod ig/init-key :d-core.core.producers.rabbitmq/producer
  [_ {:keys [rabbitmq routing codec logger]}]
  (let [channel (rc/open-channel rabbitmq)]
    (->RabbitMQProducer rabbitmq routing codec logger channel (Object.))))
