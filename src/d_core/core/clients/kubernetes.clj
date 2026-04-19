(ns d-core.core.clients.kubernetes
  (:require [integrant.core :as ig]
            [d-core.core.clients.kubernetes.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.kubernetes/client
  [_ opts]
  (impl/make-client opts))
