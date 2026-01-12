(ns d-core.core.clients.postgres
  (:require [integrant.core :as ig]
            [d-core.core.clients.postgres.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.postgres/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :d-core.core.clients.postgres/client
  [_ client]
  (impl/close! client))

