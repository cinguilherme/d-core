(ns d-core.core.clients.rabbitmq.client
  (:import (com.rabbitmq.client Connection ConnectionFactory)))

(defrecord RabbitMQClient [^Connection conn uri]
  Object
  (toString [_] (str "#RabbitMQClient{:uri " (pr-str uri) "}")))

(defn make-client
  [{:keys [uri host port username password vhost]
    :or {uri "amqp://guest:guest@localhost:5672/"}}]
  (let [^ConnectionFactory factory (ConnectionFactory.)]
    (if uri
      (.setUri factory uri)
      (do
        (.setHost factory (or host "localhost"))
        (.setPort factory (int (or port 5672)))
        (.setUsername factory (or username "guest"))
        (.setPassword factory (or password "guest"))
        (when vhost
          (.setVirtualHost factory vhost))))
    (let [^Connection conn (.newConnection factory)]
      (->RabbitMQClient conn uri))))

(defn open-channel
  ^com.rabbitmq.client.Channel
  [^RabbitMQClient client]
  (.createChannel ^Connection (:conn client)))

(defn close!
  [^RabbitMQClient client]
  (when-let [^Connection conn (:conn client)]
    (try
      (.close conn)
      (catch Exception _e
        nil))))
