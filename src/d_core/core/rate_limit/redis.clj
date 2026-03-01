(ns d-core.core.rate-limit.redis
  (:require [d-core.core.clients.redis.utils :as redis-utils]
            [d-core.core.rate-limit.protocol :as p]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(def ^:private incr-window-lua
  (str "local current = redis.call('INCRBY', KEYS[1], ARGV[1]);"
       "if current == tonumber(ARGV[1]) then "
       "  redis.call('PEXPIRE', KEYS[1], ARGV[2]);"
       "end;"
       "return current;"))

(defn- now-ms
  [clock]
  (long (clock)))

(defn- require-positive-long
  [value field]
  (let [v (long value)]
    (when (<= v 0)
      (throw (ex-info "Field must be greater than zero"
                      {:type ::invalid-rate-limit
                       :field field
                       :value value})))
    v))

(defn- window-start-ms
  [now window-ms]
  (* (quot now window-ms) window-ms))

(defn- counter-key
  [prefix key window-start]
  (str prefix key ":" window-start))

(defn increment-window!
  [redis-client redis-key amount ttl-ms]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval incr-window-lua
                      1
                      redis-key
                      (str amount)
                      (str ttl-ms))))

(defn- consume-fixed-window
  [{:keys [redis-client prefix default-limit default-window-ms clock]} key opts]
  (let [limit (require-positive-long (or (:limit opts) default-limit) :limit)
        window-ms (require-positive-long (or (:window-ms opts) default-window-ms) :window-ms)
        amount (max 1 (long (or (:amount opts) 1)))
        now (now-ms clock)
        start (window-start-ms now window-ms)
        reset-at (+ start window-ms)
        ttl-ms (max 1 (- reset-at now))
        redis-key (counter-key prefix key start)]
    (if (> amount limit)
      {:allowed? false
       :remaining 0
       :reset-at reset-at
       :retry-after-ms (max 0 (- reset-at now))}
      (let [current (long (or (increment-window! redis-client redis-key amount ttl-ms) 0))
            allowed? (<= current limit)]
        {:allowed? allowed?
         :remaining (max 0 (- limit current))
         :reset-at reset-at
         :retry-after-ms (when-not allowed? (max 0 (- reset-at now)))}))))

(defn- blocking-consume
  [limiter key opts]
  (loop []
    (let [res (p/consume! limiter key opts)]
      (if (:allowed? res)
        res
        (if-let [retry-ms (:retry-after-ms res)]
          (do
            (Thread/sleep (max 0 (long retry-ms)))
            (recur))
          res)))))

(defrecord RedisRateLimiter [redis-client prefix default-limit default-window-ms clock]
  p/RateLimitProtocol
  (consume! [_ key opts]
    (consume-fixed-window {:redis-client redis-client
                           :prefix prefix
                           :default-limit default-limit
                           :default-window-ms default-window-ms
                           :clock clock}
                          key
                          opts))
  (consume-blocking! [this key opts]
    (blocking-consume this key opts)))

(defmethod ig/init-key :d-core.core.rate-limit.redis/limiter
  [_ {:keys [redis-client prefix limit window-ms clock]
      :or {prefix "dcore:rate-limit:"
           limit 100
           window-ms 60000
           clock #(System/currentTimeMillis)}}]
  (when-not redis-client
    (throw (ex-info "Redis rate limiter requires :redis-client"
                    {:type ::missing-redis-client})))
  (->RedisRateLimiter redis-client
                      (str prefix)
                      (long limit)
                      (long window-ms)
                      clock))
