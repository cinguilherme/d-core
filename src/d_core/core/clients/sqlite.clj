(ns d-core.core.clients.sqlite
  (:require [integrant.core :as ig]
            [d-core.core.clients.sqlite.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.sqlite/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :d-core.core.clients.sqlite/client
  [_ client]
  (impl/close! client))

