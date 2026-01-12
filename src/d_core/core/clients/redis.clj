(ns d-core.core.clients.redis
  (:require [integrant.core :as ig]
            [d-core.core.clients.redis.client :as impl]))

(defmethod ig/init-key :d-core.core.clients.redis/client
  [_ opts]
  (impl/make-client opts))

