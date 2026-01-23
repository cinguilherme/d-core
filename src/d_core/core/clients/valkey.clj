(ns d-core.core.clients.valkey
  (:require [integrant.core :as ig]
            [d-core.core.clients.valkey.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.valkey/client
  [_ opts]
  (impl/make-client opts))
