(ns d-core.core.leader-election.common
  (:require [clojure.string :as str]
            [d-core.libs.time :as time])
  (:import [java.net InetAddress]
           [java.time Clock Instant]
           [java.util UUID]))

(def default-lease-ms
  15000)

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
  (let [v (cond
            (and (integer? value)
                 (instance? Number value)) (long value)
            :else nil)]
    (when-not v
      (throw (ex-info "Field must be a positive integer"
                      {:type ::invalid-field
                       :field field
                       :value value})))
    (when (<= v 0)
      (throw (ex-info "Field must be greater than zero"
                      {:type ::invalid-field
                       :field field
                       :value value})))
    v))

(defn normalize-key-part
  [value]
  (cond
    (nil? value) nil
    (string? value) value
    (keyword? value) (name value)
    (bytes? value) (String. ^bytes value "UTF-8")
    :else (str value)))

(defn normalize-election-id
  [election-id]
  (let [value (normalize-key-part election-id)]
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

(defn remaining-ttl-ms
  [expires-at-ms now-ms]
  (when (and (some? expires-at-ms) (some? now-ms))
    (max 0 (- (long expires-at-ms) (long now-ms)))))

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
