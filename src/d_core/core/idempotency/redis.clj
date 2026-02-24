(ns d-core.core.idempotency.redis
  (:require [clojure.edn :as edn]
            [d-core.core.clients.redis.utils :as redis-utils]
            [d-core.core.idempotency.protocol :as p]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn result-key
  [prefix key]
  (str prefix key ":result"))

(defn pending-key
  [prefix key]
  (str prefix key ":pending"))

(defn- encode-response
  [response]
  (pr-str response))

(defn- decode-response
  [payload]
  (when (string? payload)
    (try
      (edn/read-string payload)
      (catch Exception _
        nil))))

(defn read-result
  [redis-client key]
  (car/wcar (redis-utils/conn redis-client)
            (car/get key)))

(defn claim-pending!
  [redis-client key ttl-ms]
  (car/wcar (redis-utils/conn redis-client)
            (car/set key "1" :nx :px ttl-ms)))

(defn write-result!
  [redis-client key payload ttl-ms]
  (car/wcar (redis-utils/conn redis-client)
            (if (pos? (long ttl-ms))
              (car/psetex key ttl-ms payload)
              (car/set key payload))))

(defn delete-key!
  [redis-client key]
  (car/wcar (redis-utils/conn redis-client)
            (car/del key)))

(defrecord RedisIdempotency [redis-client prefix default-ttl-ms]
  p/IdempotencyProtocol
  (claim! [_ key ttl-ms]
    (let [ttl-ms (long (or ttl-ms default-ttl-ms 0))
          result-k (result-key prefix key)
          pending-k (pending-key prefix key)]
      (if-let [existing (some-> (read-result redis-client result-k) decode-response)]
        {:ok true :status :completed :response existing}
        (let [claimed? (= "OK" (claim-pending! redis-client pending-k ttl-ms))]
          (if claimed?
            {:ok true :status :claimed}
            (if-let [completed (some-> (read-result redis-client result-k) decode-response)]
              {:ok true :status :completed :response completed}
              {:ok true :status :in-progress}))))))

  (complete! [_ key response ttl-ms]
    (let [ttl-ms (long (or ttl-ms default-ttl-ms 0))
          result-k (result-key prefix key)
          pending-k (pending-key prefix key)]
      (write-result! redis-client result-k (encode-response response) ttl-ms)
      (delete-key! redis-client pending-k)
      {:ok true :status :completed :response response}))

  (lookup [_ key]
    (let [result-k (result-key prefix key)]
      (when-let [payload (read-result redis-client result-k)]
        {:ok true :status :completed :response (decode-response payload)}))))

(defmethod ig/init-key :d-core.core.idempotency.redis/redis
  [_ {:keys [redis-client prefix default-ttl-ms]
      :or {prefix "dcore:idempotency:" default-ttl-ms 21600000}}]
  (->RedisIdempotency redis-client prefix default-ttl-ms))
