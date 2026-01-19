(ns d-core.core.clients.memcached
  (:require [integrant.core :as ig]
            [d-core.core.clients.memcached.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.memcached/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :d-core.core.clients.memcached/client
  [_ client]
  (impl/close! client))
