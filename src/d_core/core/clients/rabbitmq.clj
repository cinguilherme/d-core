(ns d-core.core.clients.rabbitmq
  (:require [integrant.core :as ig]
            [d-core.core.clients.rabbitmq.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.rabbitmq/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :d-core.core.clients.rabbitmq/client
  [_ client]
  (impl/close! client))
