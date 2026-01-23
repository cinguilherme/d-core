(ns d-core.core.clients.sqs
  (:require [integrant.core :as ig]
            [d-core.core.clients.sqs.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.sqs/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :d-core.core.clients.sqs/client
  [_ client]
  (impl/close! client))
