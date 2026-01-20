(ns d-core.core.clients.tile38
  (:require [integrant.core :as ig]
            [d-core.core.clients.tile38.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.tile38/client
  [_ opts]
  (impl/make-client opts))
