(ns d-core.core.clients.datomic
  (:require [integrant.core :as ig]
            [d-core.core.clients.datomic.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.datomic/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :d-core.core.clients.datomic/client
  [_ client]
  (impl/close! client))
