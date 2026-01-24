(ns d-core.core.cache.layered
  (:require [clojure.edn :as edn]
            [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.cache.protocol :as p]
            [d-core.core.codecs.protocol :as codec]
            [d-core.core.compression.protocol :as compression]))

(import (java.io ByteArrayOutputStream)
        (java.nio ByteBuffer))

(defn- safe-log
  [l level event data]
  (when l
    (logger/log l level event data)))

(defn- miss?
  [layered value]
  (or (nil? value) (= value (:miss layered))))

(def ^:private chunk-header-bytes
  (.getBytes "DCORE_CHUNKED\0" "UTF-8"))

(defn- byte-buffer->bytes
  [^ByteBuffer buf]
  (let [dup (.duplicate buf)
        arr (byte-array (.remaining dup))]
    (.get dup arr)
    arr))

(defn- payload->bytes
  [payload]
  (cond
    (nil? payload) nil
    (bytes? payload) payload
    (instance? ByteBuffer payload) (byte-buffer->bytes payload)
    (string? payload) (.getBytes ^String payload "UTF-8")
    :else nil))

(defn- has-chunk-header?
  [^bytes bytes]
  (let [hlen (alength ^bytes chunk-header-bytes)]
    (when (and bytes (>= (alength bytes) hlen))
      (loop [idx 0]
        (cond
          (= idx hlen) true
          (= (aget ^bytes bytes idx) (aget ^bytes chunk-header-bytes idx)) (recur (inc idx))
          :else false)))))

(defn- chunk-manifest-bytes
  [manifest]
  (let [payload (.getBytes (pr-str manifest) "UTF-8")
        out (ByteArrayOutputStream.)]
    (.write out ^bytes chunk-header-bytes)
    (.write out ^bytes payload)
    (.toByteArray out)))

(defn- parse-chunk-manifest
  [^bytes bytes]
  (let [hlen (alength ^bytes chunk-header-bytes)
        payload (String. ^bytes bytes hlen (- (alength bytes) hlen) "UTF-8")]
    (edn/read-string payload)))

(defn- chunk-key
  [key idx]
  (str key "::chunk/" idx))

(defn- concat-bytes
  [chunks]
  (let [out (ByteArrayOutputStream.)]
    (doseq [^bytes chunk chunks]
      (.write out chunk))
    (.toByteArray out)))

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

(defn- encode-value
  [tier value]
  (if-let [c (:codec tier)]
    (codec/encode c value)
    value))

(defn- decode-value
  [tier payload]
  (if-let [c (:codec tier)]
    (codec/decode c payload)
    payload))

(defn- compress-bytes
  [tier bytes]
  (if-let [c (:compressor tier)]
    (compression/compress c bytes)
    bytes))

(defn- decompress-bytes
  [tier bytes]
  (if-let [c (:compressor tier)]
    (compression/decompress c bytes)
    bytes))

(defn- chunk-size
  [tier]
  (or (:chunk-bytes tier) (:max-value-bytes tier)))

(defn- max-bytes
  [tier]
  (or (:max-value-bytes tier) (:chunk-bytes tier)))

(defn- prepare-bytes
  [tier value]
  (let [encoded (encode-value tier value)
        needs-bytes? (or (:compressor tier)
                         (:max-value-bytes tier)
                         (:chunk-bytes tier))
        bytes (when needs-bytes?
                (or (payload->bytes encoded)
                    (throw (ex-info "Tier requires byte-serializable values"
                                    {:tier (:id tier)
                                     :value-type (type encoded)}))))
        compressed (when bytes (compress-bytes tier bytes))]
    {:encoded encoded
     :bytes compressed}))

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

(defn- write-chunked!
  [tier key bytes opts]
  (let [chunk-bytes (chunk-size tier)
        total (alength ^bytes bytes)
        chunk-count (int (Math/ceil (/ (double total) (double chunk-bytes))))
        manifest {:chunked? true
                  :chunks chunk-count
                  :chunk-bytes chunk-bytes
                  :bytes total}
        tier-opts (cache-opts opts tier)]
    (p/cache-put (:cache tier) key (chunk-manifest-bytes manifest) tier-opts)
    (dotimes [idx chunk-count]
      (let [start (* idx chunk-bytes)
            end (min total (+ start chunk-bytes))
            chunk (java.util.Arrays/copyOfRange ^bytes bytes start end)]
        (p/cache-put (:cache tier) (chunk-key key idx) chunk tier-opts)))))

(defn- read-chunked
  [tier key manifest opts]
  (let [chunks (:chunks manifest)]
    (when (and (integer? chunks) (pos? chunks))
      (let [cache-opts (clean-opts opts)
            chunk-bytes (for [idx (range chunks)]
                          (let [chunk (p/cache-lookup (:cache tier) (chunk-key key idx) cache-opts)]
                            (payload->bytes chunk)))]
        (when (every? some? chunk-bytes)
          (concat-bytes chunk-bytes))))))

(defn- write-to-tier
  [tier key value opts]
  (let [{:keys [encoded bytes]} (prepare-bytes tier value)
        max-bytes (max-bytes tier)
        size (when bytes (alength ^bytes bytes))]
    (cond
      (and max-bytes size (> size max-bytes) (chunk-size tier))
      (write-chunked! tier key bytes opts)

      bytes
      (p/cache-put (:cache tier) key bytes (cache-opts opts tier))

      :else
      (p/cache-put (:cache tier) key encoded (cache-opts opts tier)))))

(defn- read-from-tier
  [tier key opts]
  (let [cache-opts (clean-opts opts)
        payload (p/cache-lookup (:cache tier) key cache-opts)]
    (when-not (nil? payload)
      (let [payload-bytes (payload->bytes payload)]
        (if (and payload-bytes (has-chunk-header? payload-bytes))
          (let [manifest (parse-chunk-manifest payload-bytes)
                chunked (and (map? manifest) (:chunked? manifest))
                bytes (when chunked (read-chunked tier key manifest opts))]
            (when bytes
              (let [decompressed (decompress-bytes tier bytes)]
                (decode-value tier decompressed))))
          (let [value (if-let [c (:compressor tier)]
                        (let [bytes (or payload-bytes
                                        (throw (ex-info "Compressed payload is not byte-addressable"
                                                        {:tier (:id tier)})))
                              decompressed (compression/decompress c bytes)]
                          (decode-value tier decompressed))
                        (decode-value tier payload))]
            value))))))

(defn- delete-chunked
  [tier key opts]
  (let [cache-opts (clean-opts opts)
        payload (p/cache-lookup (:cache tier) key cache-opts)
        payload-bytes (payload->bytes payload)]
    (when (and payload-bytes (has-chunk-header? payload-bytes))
      (let [manifest (parse-chunk-manifest payload-bytes)
            chunks (:chunks manifest)]
        (when (and (integer? chunks) (pos? chunks))
          (doseq [idx (range chunks)]
            (p/cache-delete (:cache tier) (chunk-key key idx) cache-opts)))))))

(defn- promote-tiers
  [layered tiers key value opts]
  (doseq [tier tiers
          :when (and (promote-enabled? tier) (write-enabled? tier))]
    (write-to-tier tier key value opts))
  value)

(defn- write-tiers
  [layered tiers key value opts]
  (doseq [tier tiers
          :when (write-enabled? tier)]
    (write-to-tier tier key value opts))
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
            (let [value (read-from-tier tier key cache-opts)]
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
        (delete-chunked tier key opts)
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
