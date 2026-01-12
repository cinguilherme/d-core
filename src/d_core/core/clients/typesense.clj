(ns d-core.core.clients.typesense
  (:require [integrant.core :as ig]
            [d-core.core.clients.typesense.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.typesense/client
  [_ opts]
  (impl/make-client opts))

