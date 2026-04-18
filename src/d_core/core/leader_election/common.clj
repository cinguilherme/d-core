(ns d-core.core.leader-election.common
  (:require [clojure.string :as str]
            [d-core.core.clients.redis.utils :as redis-utils]
            [d-core.libs.time :as time])
  (:import [java.net InetAddress]
           [java.time Clock Instant]
           [java.util UUID]))

(def default-prefix
  "dcore:leader-election:")

(def default-lease-ms
  15000)

(def ^:private shared-script-preamble
  "local call = redis and redis.call or server.call;")

(def acquire-lua
  (str shared-script-preamble
       "if call('EXISTS', KEYS[1]) == 0 then "
       "  local fencing = call('INCR', KEYS[2]);"
       "  call('HSET', KEYS[1], "
       "       'owner_id', ARGV[1], "
       "       'token', ARGV[2], "
       "       'fencing', tostring(fencing), "
       "       'acquired_at_ms', ARGV[3], "
       "       'renewed_at_ms', ARGV[3]);"
       "  call('PEXPIRE', KEYS[1], ARGV[4]);"
       "  return {'acquired', ARGV[1], tostring(fencing), ARGV[2], ARGV[4]};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "return {'busy', values[1], values[2], '', tostring(ttl)};"))

(def renew-lua
  (str shared-script-preamble
       "local current = call('HGET', KEYS[1], 'token');"
       "if not current then "
       "  return {'lost'};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "if current ~= ARGV[1] then "
       "  local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "  return {'lost', values[1], values[2], tostring(ttl)};"
       "end;"
       "call('HSET', KEYS[1], 'renewed_at_ms', ARGV[2]);"
       "call('PEXPIRE', KEYS[1], ARGV[3]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "return {'renewed', values[1], values[2], ARGV[1], ARGV[3]};"))

(def resign-lua
  (str shared-script-preamble
       "local current = call('HGET', KEYS[1], 'token');"
       "if not current then "
       "  return {'not-owner'};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "if current ~= ARGV[1] then "
       "  return {'not-owner', values[1], values[2], tostring(ttl)};"
       "end;"
       "call('DEL', KEYS[1]);"
       "return {'released', values[1], values[2]};"))

(def status-lua
  (str shared-script-preamble
       "if call('EXISTS', KEYS[1]) == 0 then "
       "  return {'vacant'};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "return {'held', values[1], values[2], tostring(ttl)};"))

(defn default-clock
  []
  (time/new-clock {:type :system :zone "UTC"}))

(defn normalize-clock
  [clock]
  (cond
    (nil? clock) (default-clock)
    (fn? clock) clock
    (instance? Clock clock) clock
    (map? clock) (time/new-clock clock)
    :else
    (throw (ex-info "Unsupported clock value"
                    {:type ::invalid-clock
                     :clock clock}))))

(defn now-ms
  [clock]
  (let [value (if (fn? clock)
                (clock)
                (time/now clock))]
    (cond
      (number? value) (long value)
      (instance? Instant value) (.toEpochMilli ^Instant value)
      :else
      (throw (ex-info "Clock must return epoch ms or Instant"
                      {:type ::invalid-clock-value
                       :value value})))))

(defn require-positive-long
  [value field]
  (let [v (long value)]
    (when (<= v 0)
      (throw (ex-info "Field must be greater than zero"
                      {:type ::invalid-field
                       :field field
                       :value value})))
    v))

(defn normalize-prefix
  [prefix]
  (let [value (str (or prefix default-prefix))]
    (when (str/blank? value)
      (throw (ex-info "Leader election prefix must not be blank"
                      {:type ::invalid-prefix
                       :prefix prefix})))
    value))

(defn normalize-election-id
  [election-id]
  (let [value (when (some? election-id)
                (redis-utils/normalize-key election-id))]
    (when (str/blank? value)
      (throw (ex-info "Leader election requires a non-blank election id"
                      {:type ::invalid-election-id
                       :election-id election-id})))
    value))

(defn normalize-token
  [token]
  (let [value (when (some? token) (str token))]
    (when (str/blank? value)
      (throw (ex-info "Leader election requires a non-blank token"
                      {:type ::invalid-token
                       :token token})))
    value))

(defn generate-token
  []
  (str (UUID/randomUUID)))

(defn build-owner-id
  []
  (let [hostname (try
                   (.getHostName (InetAddress/getLocalHost))
                   (catch Exception _
                     "unknown-host"))]
    (str hostname ":" (UUID/randomUUID))))

(defn normalize-owner-id
  [owner-id]
  (let [value (str (or owner-id (build-owner-id)))]
    (when (str/blank? value)
      (throw (ex-info "Leader election owner id must not be blank"
                      {:type ::invalid-owner-id
                       :owner-id owner-id})))
    value))

(defn lease-ms
  [opts default-lease-ms]
  (require-positive-long (or (:lease-ms opts) default-lease-ms) :lease-ms))

(defn lease-key
  [prefix election-id]
  (str prefix election-id ":lease"))

(defn fencing-key
  [prefix election-id]
  (str prefix election-id ":fencing"))

(defn parse-long-safe
  [value]
  (when (some? value)
    (try
      (Long/parseLong (str value))
      (catch Exception _
        nil))))

(defn normalize-ttl-ms
  [ttl-ms]
  (let [value (parse-long-safe ttl-ms)]
    (when (and (some? value) (>= value 0))
      value)))

(defn- status-keyword
  [raw allowed]
  (let [value (keyword (str raw))]
    (when-not (contains? allowed value)
      (throw (ex-info "Unexpected leader election status"
                      {:type ::unexpected-status
                       :status raw
                       :allowed allowed})))
    value))

(defn- base-result
  [backend election-id status]
  {:ok true
   :status status
   :backend backend
   :election-id election-id})

(defn- assoc-holder-meta
  [result owner-id fencing remaining-ttl-ms]
  (cond-> result
    (some? owner-id) (assoc :owner-id owner-id)
    (some? fencing) (assoc :fencing fencing)
    (some? remaining-ttl-ms) (assoc :remaining-ttl-ms remaining-ttl-ms)))

(defn acquire-result
  [backend election-id response]
  (let [[raw-status owner-id raw-fencing token raw-ttl] (vec (or response []))
        status (status-keyword raw-status #{:acquired :busy})
        result (assoc-holder-meta (base-result backend election-id status)
                                  owner-id
                                  (parse-long-safe raw-fencing)
                                  (normalize-ttl-ms raw-ttl))]
    (if (= :acquired status)
      (assoc result :token token)
      result)))

(defn renew-result
  [backend election-id response]
  (let [parts (vec (or response []))
        raw-status (nth parts 0 nil)
        status (status-keyword raw-status #{:renewed :lost})
        owner-id (nth parts 1 nil)
        raw-fencing (nth parts 2 nil)
        token (when (= :renewed status) (nth parts 3 nil))
        raw-ttl (if (= :renewed status)
                  (nth parts 4 nil)
                  (nth parts 3 nil))
        result (assoc-holder-meta (base-result backend election-id status)
                                  owner-id
                                  (parse-long-safe raw-fencing)
                                  (normalize-ttl-ms raw-ttl))]
    (if (= :renewed status)
      (assoc result :token token)
      result)))

(defn resign-result
  [backend election-id response]
  (let [[raw-status owner-id raw-fencing raw-ttl] (vec (or response []))
        status (status-keyword raw-status #{:released :not-owner})]
    (assoc-holder-meta (base-result backend election-id status)
                       owner-id
                       (parse-long-safe raw-fencing)
                       (normalize-ttl-ms raw-ttl))))

(defn status-result
  [backend election-id response]
  (let [[raw-status owner-id raw-fencing raw-ttl] (vec (or response []))
        status (status-keyword raw-status #{:vacant :held})]
    (assoc-holder-meta (base-result backend election-id status)
                       owner-id
                       (parse-long-safe raw-fencing)
                       (normalize-ttl-ms raw-ttl))))
