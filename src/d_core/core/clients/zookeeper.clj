(ns d-core.core.clients.zookeeper
  (:require [d-core.core.clients.zookeeper.client :as impl]
            [integrant.core :as ig]))

(defmethod ig/init-key :d-core.core.clients.zookeeper/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :d-core.core.clients.zookeeper/client
  [_ client]
  (impl/close! client))
