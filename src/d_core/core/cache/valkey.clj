(ns d-core.core.cache.valkey
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [d-core.core.cache.protocol :as p]
            [d-core.core.clients.valkey.client]))

(defn valkey-get
  [valkey-client key]
  (car/wcar (:conn valkey-client)
    (car/get key)))

(defn valkey-set
  [valkey-client key value]
  (car/wcar (:conn valkey-client)
    (car/set key value)))

(defn valkey-setex
  [valkey-client key ttl value]
  (car/wcar (:conn valkey-client)
    (car/setex key ttl value)))

(defn valkey-del
  [valkey-client key]
  (car/wcar (:conn valkey-client)
    (car/del key)))

(defn valkey-flushdb
  [valkey-client]
  (car/wcar (:conn valkey-client)
    (car/flushdb)))

(defrecord ValkeyCache [valkey-client]
  p/CacheProtocol
  (cache-lookup [_ key _opts]
    (valkey-get valkey-client key))
  (cache-put [_ key value opts]
    (if-let [ttl (:ttl opts)]
      (valkey-setex valkey-client key ttl value)
      (valkey-set valkey-client key value)))
  (cache-delete [_ key _opts]
    (valkey-del valkey-client key))
  (cache-clear [_ _opts]
    (valkey-flushdb valkey-client)))

(defmethod ig/init-key :d-core.core.cache.valkey/valkey
  [_ {:keys [valkey-client]}]
  (->ValkeyCache valkey-client))
