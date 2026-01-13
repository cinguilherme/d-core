(ns d-core.core.producers.redis
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [duct.logger :as logger]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.messaging.routing :as routing]
            [d-core.core.producers.protocol :as p]
            [d-core.core.clients.redis.client]))

(defrecord RedisStreamsProducer [redis-client routing codec logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          trace (:trace options)
          topic-cfg (routing/topic-config routing topic)
          stream (or (:stream options)
                     (:stream topic-cfg)
                     (str "core:" (name topic)))
          envelope {:msg msg-map
                    :options options
                    :metadata (cond-> {}
                                trace (assoc :trace trace))
                    :produced-at (System/currentTimeMillis)}
          payload (codec/encode codec envelope)
          _ (logger/log logger :info ::producing-message {:topic topic :stream stream :trace trace})
          id (car/wcar (:conn redis-client)
               (car/xadd stream "*" "payload" payload))]
      {:ok true
       :backend :redis
       :topic topic
       :stream stream
       :id id})))

(defmethod ig/init-key :d-core.core.producers.redis/producer
  [_ {:keys [redis routing codec logger]}]
  (logger/log logger :info ::initializing-producer (str {:flavor :redis
                                                         :routing routing
                                                         :codec codec}))
  (->RedisStreamsProducer redis routing codec logger))

