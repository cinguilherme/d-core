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

(def ^:private claim-or-read-result-lua
  (str
   "local result = redis.call('GET', KEYS[1]);"
   "if result then return {2, result}; end;"
   "local claimed = redis.call('SET', KEYS[2], '1', 'NX', 'PX', ARGV[1]);"
   "if claimed then return {1}; end;"
   "local completed = redis.call('GET', KEYS[1]);"
   "if completed then return {2, completed}; end;"
   "return {0};"))

(defn claim-or-read-result!
  [redis-client result-key pending-key ttl-ms]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval claim-or-read-result-lua
                      2
                      result-key
                      pending-key
                      (str ttl-ms))))

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
          pending-k (pending-key prefix key)
          [status-code payload] (or (claim-or-read-result! redis-client result-k pending-k ttl-ms)
                                    [0 nil])
          status-code (long (or status-code 0))]
      (cond
        (= 2 status-code)
        {:ok true
         :status :completed
         :response (decode-response payload)}

        (= 1 status-code)
        {:ok true :status :claimed}

        :else
        {:ok true :status :in-progress})))

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
      ;; Default TTL is 6 hours (21600000 ms). This is long enough to cover typical
      ;; request retry windows in our system while limiting Redis memory usage.
      ;; Override `default-ttl-ms` in configuration if your use case requires a
      ;; shorter or longer idempotency window.
      :or {prefix "dcore:idempotency:" default-ttl-ms 21600000}}]
  (->RedisIdempotency redis-client prefix default-ttl-ms))
