(ns d-core.core.cache.layered
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.cache.protocol :as p]))

(defn- safe-log
  [l level event data]
  (when l
    (logger/log l level event data)))

(defn- miss?
  [layered value]
  (or (nil? value) (= value (:miss layered))))

(defn- ttl-ms
  [ttl ttl-unit]
  (cond
    (nil? ttl) nil
    (map? ttl) (ttl-ms (:value ttl) (:unit ttl))
    (number? ttl)
    (case (or ttl-unit :ms)
      :ms ttl
      :millis ttl
      :s (* ttl 1000)
      :sec (* ttl 1000)
      :seconds (* ttl 1000)
      :m (* ttl 60000)
      :min (* ttl 60000)
      :minutes (* ttl 60000)
      :h (* ttl 3600000)
      :hours (* ttl 3600000)
      (throw (ex-info "Unsupported TTL unit" {:ttl ttl :ttl-unit ttl-unit})))
    :else (throw (ex-info "Unsupported TTL value" {:ttl ttl :ttl-unit ttl-unit}))))

(defn- ttl-ms-from
  [opts tier]
  (or (:ttl-ms opts)
      (ttl-ms (:ttl opts) (:ttl-unit opts))
      (:ttl-ms tier)
      (ttl-ms (:ttl tier) (:ttl-unit tier))))

(defn- ttl-ms->unit
  [ttl-ms unit]
  (let [ms (long ttl-ms)
        scale (case unit
                :ms 1
                :millis 1
                :s 1000
                :sec 1000
                :seconds 1000
                :m 60000
                :min 60000
                :minutes 60000
                :h 3600000
                :hours 3600000
                (throw (ex-info "Unsupported TTL unit" {:ttl-ms ttl-ms :ttl-unit unit})))]
    (long (Math/ceil (/ (double ms) (double scale))))))

(defn- ttl->client
  [tier ttl-ms]
  (when (and ttl-ms (pos? ttl-ms))
    (let [coerce (:ttl-coerce tier)
          ttl-unit (:ttl-unit tier)
          ttl (cond
                coerce (coerce ttl-ms)
                ttl-unit (ttl-ms->unit ttl-ms ttl-unit)
                :else (ttl-ms->unit ttl-ms :sec))]
      (when (and ttl (pos? ttl))
        {:ttl ttl}))))

(defn- clean-opts
  [opts]
  (when opts
    (dissoc opts :ttl :ttl-ms :ttl-unit)))

(defn- cache-opts
  [opts tier]
  (let [ttl-ms (ttl-ms-from opts tier)]
    (merge (clean-opts opts) (ttl->client tier ttl-ms))))

(defn- read-enabled?
  [tier]
  (not (false? (get tier :read? true))))

(defn- write-enabled?
  [tier]
  (not (false? (get tier :write? true))))

(defn- promote-enabled?
  [tier]
  (true? (get tier :promote? false)))

(defn- source-read
  [source key opts]
  (cond
    (nil? source) nil
    (:read-fn source) ((:read-fn source) key opts)
    (:cache source) (p/cache-lookup (:cache source) key opts)
    (satisfies? p/CacheProtocol source) (p/cache-lookup source key opts)
    :else nil))

(defn- source-write
  [source key value opts]
  (cond
    (nil? source) nil
    (:write-fn source) ((:write-fn source) key value opts)
    (:cache source) (p/cache-put (:cache source) key value opts)
    (satisfies? p/CacheProtocol source) (p/cache-put source key value opts)
    :else nil))

(defn- source-delete
  [source key opts]
  (cond
    (nil? source) nil
    (:delete-fn source) ((:delete-fn source) key opts)
    (:cache source) (p/cache-delete (:cache source) key opts)
    (satisfies? p/CacheProtocol source) (p/cache-delete source key opts)
    :else nil))

(defn- promote-tiers
  [layered tiers key value opts]
  (doseq [tier tiers
          :when (and (promote-enabled? tier) (write-enabled? tier))]
    (p/cache-put (:cache tier) key value (cache-opts opts tier)))
  value)

(defn- write-tiers
  [layered tiers key value opts]
  (doseq [tier tiers
          :when (write-enabled? tier)]
    (p/cache-put (:cache tier) key value (cache-opts opts tier)))
  value)

(defrecord LayeredCache [tiers source write-strategy miss logger]
  p/CacheProtocol
  (cache-lookup [this key opts]
    (let [opts (or opts {})
          cache-opts (clean-opts opts)]
      (loop [remaining tiers
             visited []]
        (if-let [tier (first remaining)]
          (if (read-enabled? tier)
            (let [value (p/cache-lookup (:cache tier) key cache-opts)]
              (if (miss? this value)
                (recur (rest remaining) (conj visited tier))
                (do
                  (safe-log logger :debug ::cache-hit {:key key :tier (:id tier)})
                  (promote-tiers this visited key value opts)
                  value)))
            (recur (rest remaining) (conj visited tier)))
          (let [value (source-read source key opts)]
            (if (miss? this value)
              (do
                (safe-log logger :debug ::cache-miss {:key key})
                nil)
              (do
                (safe-log logger :debug ::cache-source-hit {:key key})
                (write-tiers this tiers key value opts)
                value)))))))
  (cache-put [this key value opts]
    (let [opts (or opts {})]
      (if (miss? this value)
        (do
          (safe-log logger :debug ::cache-put-miss {:key key})
          nil)
        (do
          (when (or (= :write-through write-strategy)
                    (= :write-around write-strategy))
            (source-write source key value opts))
          (when (not= :write-around write-strategy)
            (write-tiers this tiers key value opts))
          value))))
  (cache-delete [_ key opts]
    (let [opts (or opts {})]
      (doseq [tier tiers
              :when (write-enabled? tier)]
        (p/cache-delete (:cache tier) key (clean-opts opts)))
      (source-delete source key opts)
      nil))
  (cache-clear [_ opts]
    (let [opts (or opts {})]
      (doseq [tier tiers
              :when (write-enabled? tier)]
        (p/cache-clear (:cache tier) (clean-opts opts)))
      nil)))

(defmethod ig/init-key :d-core.core.cache.layered/layered
  [_ {:keys [logger tiers source write-strategy miss]
      :or {write-strategy :write-through
           miss ::miss}}]
  (safe-log logger :info ::initializing-layered-cache
            {:tiers (mapv :id tiers)
             :write-strategy write-strategy})
  (->LayeredCache tiers source write-strategy miss logger))
