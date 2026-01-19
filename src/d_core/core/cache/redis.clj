(ns d-core.core.cache.redis
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [d-core.core.cache.protocol :as p]
            [d-core.core.clients.redis.client]))

(defn redis-get
  [redis-client key]
  (car/wcar (:conn redis-client)
    (car/get key)))

(defn redis-set
  [redis-client key value]
  (car/wcar (:conn redis-client)
    (car/set key value)))

(defn redis-setex
  [redis-client key ttl value]
  (car/wcar (:conn redis-client)
    (car/setex key ttl value)))

(defn redis-del
  [redis-client key]
  (car/wcar (:conn redis-client)
    (car/del key)))

(defn redis-flushdb
  [redis-client]
  (car/wcar (:conn redis-client)
    (car/flushdb)))

(defrecord RedisCache [redis-client]
  p/CacheProtocol
  (cache-lookup [_ key _opts]
    (redis-get redis-client key))
  (cache-put [_ key value opts]
    (if-let [ttl (:ttl opts)]
      (redis-setex redis-client key ttl value)
      (redis-set redis-client key value)))
  (cache-delete [_ key _opts]
    (redis-del redis-client key))
  (cache-clear [_ _opts]
    (redis-flushdb redis-client)))

(defmethod ig/init-key :d-core.core.cache.redis/redis
  [_ {:keys [redis-client]}]
  (->RedisCache redis-client))
