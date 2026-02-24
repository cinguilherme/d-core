(ns d-core.core.state-store.redis
  (:require [d-core.core.clients.redis.utils :as redis-utils]
            [d-core.core.state-store.protocol :as p]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(def ^:private set-max-field-lua
  (str "local current = redis.call('HGET', KEYS[1], ARGV[1]);"
       "if (not current) or (tonumber(ARGV[2]) > tonumber(current)) then "
       "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);"
       "return 1;"
       "end;"
       "return 0;"))

(defn hset!
  [redis-client key field value]
  (car/wcar (redis-utils/conn redis-client)
            (car/hset key field value)))

(defn hset-many!
  [redis-client key field->value]
  (let [flattened (mapcat identity field->value)]
    (car/wcar (redis-utils/conn redis-client)
              (apply car/hset key flattened))))

(defn hget
  [redis-client key field]
  (car/wcar (redis-utils/conn redis-client)
            (car/hget key field)))

(defn hgetall
  [redis-client key]
  (car/wcar (redis-utils/conn redis-client)
            (car/hgetall key)))

(defn hdel!
  [redis-client key fields]
  (if (seq fields)
    (car/wcar (redis-utils/conn redis-client)
              (apply car/hdel key fields))
    0))

(defn eval-set-max!
  [redis-client key field value]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval set-max-field-lua 1 key field value)))

(defn pexpire!
  [redis-client key ttl-ms]
  (car/wcar (redis-utils/conn redis-client)
            (car/pexpire key ttl-ms)))

(defn zadd-score!
  [redis-client key score member]
  (car/wcar (redis-utils/conn redis-client)
            (car/zadd key score member)))

(defn zcount-range
  [redis-client key min-score max-score]
  (car/wcar (redis-utils/conn redis-client)
            (car/zcount key min-score max-score)))

(defrecord RedisStateStore [redis-client]
  p/StateStoreProtocol
  (put-field! [_ key field value {:keys [ttl-ms]}]
    (let [result (hset! redis-client key field value)]
      (when (and ttl-ms (pos? (long ttl-ms)))
        (pexpire! redis-client key (long ttl-ms)))
      result))

  (put-fields! [_ key field->value {:keys [ttl-ms]}]
    (let [result (if (seq field->value)
                   (hset-many! redis-client key field->value)
                   0)]
      (when (and ttl-ms (pos? (long ttl-ms)))
        (pexpire! redis-client key (long ttl-ms)))
      result))

  (get-field [_ key field _opts]
    (hget redis-client key field))

  (get-all [_ key _opts]
    (redis-utils/fields->map (hgetall redis-client key)))

  (delete-fields! [_ key fields _opts]
    (hdel! redis-client key fields))

  (set-max-field! [_ key field value {:keys [ttl-ms]}]
    (let [updated? (= 1 (eval-set-max! redis-client key field (str value)))]
      (when (and ttl-ms (pos? (long ttl-ms)))
        (pexpire! redis-client key (long ttl-ms)))
      updated?))

  (expire! [_ key ttl-ms _opts]
    (pexpire! redis-client key (long ttl-ms)))

  (zadd! [_ key score member _opts]
    (zadd-score! redis-client key score member))

  (zcount [_ key min-score max-score _opts]
    (zcount-range redis-client key min-score max-score)))

(defmethod ig/init-key :d-core.core.state-store.redis/redis
  [_ {:keys [redis-client]}]
  (->RedisStateStore redis-client))
