(ns d-core.core.cache.memcached
  (:require [integrant.core :as ig]
            [d-core.core.cache.protocol :as p]
            [d-core.core.clients.memcached.client :as mc]))

(defrecord MemcachedCache [memcached-client]
  p/CacheProtocol
  (cache-lookup [_ key _opts]
    (mc/get* memcached-client key))
  (cache-put [_ key value opts]
    (let [ttl (:ttl opts)]
      (mc/set* memcached-client key ttl value)
      value))
  (cache-delete [_ key _opts]
    (mc/delete* memcached-client key)
    nil)
  (cache-clear [_ _opts]
    (mc/flush* memcached-client)
    nil))

(defmethod ig/init-key :d-core.core.cache.memcached/memcached
  [_ {:keys [memcached-client]}]
  (->MemcachedCache memcached-client))
