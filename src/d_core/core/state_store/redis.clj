(ns d-core.core.state-store.redis
  (:require [d-core.core.clients.redis.utils :as redis-utils]
            [d-core.core.state-store.protocol :as p]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(def ^:private set-max-field-lua
  (str "local current = redis.call('HGET', KEYS[1], ARGV[1]);"
       "local next_val = tonumber(ARGV[2]);"
       "if not next_val then return redis.error_reply('ERR state-store set-max requires numeric value'); end;"
       "local ttl_ms = tonumber(ARGV[3]) or 0;"
       "local current_num = nil;"
       "if current then "
       "current_num = tonumber(current);"
       "if not current_num then return redis.error_reply('ERR state-store set-max found non-numeric current value'); end;"
       "end;"
       "if (not current_num) or (next_val > current_num) then "
       "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);"
       "if ttl_ms > 0 then redis.call('PEXPIRE', KEYS[1], ARGV[3]); end;"
       "return 1;"
       "end;"
       "if ttl_ms > 0 then redis.call('PEXPIRE', KEYS[1], ARGV[3]); end;"
       "return 0;"))

(def ^:private numeric-value-pattern #"^-?\d+(\.\d+)?$")

(defn parse-numeric-value
  [value]
  (cond
    (number? value) (str value)
    (and (string? value)
         (re-matches numeric-value-pattern value)) value
    :else nil))

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
  [redis-client key field value ttl-ms]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval set-max-field-lua 1 key field value (str (long (or ttl-ms 0))))))

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
    (if-let [numeric-value (parse-numeric-value value)]
      (= 1 (eval-set-max! redis-client key field numeric-value ttl-ms))
      (throw (ex-info "state-store set-max requires numeric value"
                      {:key key
                       :field field
                       :value value
                       :value-type (some-> value class .getName)}))))

  (expire! [_ key ttl-ms _opts]
    (pexpire! redis-client key (long ttl-ms)))

  (zadd! [_ key score member _opts]
    (zadd-score! redis-client key score member))

  (zcount [_ key min-score max-score _opts]
    (zcount-range redis-client key min-score max-score)))

(defmethod ig/init-key :d-core.core.state-store.redis/redis
  [_ {:keys [redis-client]}]
  (->RedisStateStore redis-client))
