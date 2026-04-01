(ns d-core.core.geocoding.cached
  (:require [d-core.core.cache.protocol :as cache]
            [d-core.core.geocoding.protocol :as p]
            [d-core.core.geocoding.schema :as schema]
            [integrant.core :as ig])
  (:import (java.security MessageDigest)))

(def ^:private default-hit-ttl-ms (* 30 24 60 60 1000))
(def ^:private default-empty-ttl-ms (* 60 60 1000))

(defn- bytes->hex
  [^bytes bytes]
  (apply str (map #(format "%02x" (bit-and % 0xff)) bytes)))

(defn- sha-256
  [value]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (.update digest (.getBytes (str value) "UTF-8"))
    (bytes->hex (.digest digest))))

(defn- canonicalize
  [value]
  (cond
    (map? value)
    [:map (mapv (fn [[k v]]
                  [k (canonicalize v)])
                (sort-by (comp str key) value))]

    (vector? value)
    [:vector (mapv canonicalize value)]

    (set? value)
    [:set (mapv canonicalize (sort-by pr-str value))]

    (sequential? value)
    [:seq (mapv canonicalize value)]

    :else value))

(defn- cache-key
  [id operation request]
  (str "dcore:geocoding:" (name id) ":" (name operation) ":" (sha-256 (pr-str (canonicalize request)))))

(defn- ttl-opts
  [ttl-ms]
  (let [ttl-ms (long ttl-ms)
        ttl-sec (long (Math/ceil (/ (double ttl-ms) 1000.0)))]
    {:ttl-ms ttl-ms
     :ttl ttl-sec
     :ttl-unit :sec}))

(defn- result-empty?
  [result]
  (empty? (:items result)))

(defrecord CachedGeocoder [id geocoder cache hit-ttl-ms empty-ttl-ms]
  p/GeocodingProtocol
  (geocode [_ query opts]
    (let [query (schema/validate-geocode-query! query)
          key (cache-key id :geocode query)]
      (if-some [cached (cache/cache-lookup cache key nil)]
        (schema/validate-result-envelope! cached)
        (let [result (schema/validate-result-envelope!
                      (p/geocode geocoder query opts))
              ttl-ms (if (result-empty? result) empty-ttl-ms hit-ttl-ms)]
          (cache/cache-put cache key result (ttl-opts ttl-ms))
          result))))

  (reverse-geocode [_ location opts]
    (let [location (schema/validate-reverse-geocode-input! location)
          key (cache-key id :reverse-geocode location)]
      (if-some [cached (cache/cache-lookup cache key nil)]
        (schema/validate-result-envelope! cached)
        (let [result (schema/validate-result-envelope!
                      (p/reverse-geocode geocoder location opts))
              ttl-ms (if (result-empty? result) empty-ttl-ms hit-ttl-ms)]
          (cache/cache-put cache key result (ttl-opts ttl-ms))
          result)))))

(defmethod ig/init-key :d-core.core.geocoding.cached/geocoder
  [_ {:keys [id geocoder cache hit-ttl-ms empty-ttl-ms] :as opts}]
  (when-not geocoder
    (throw (ex-info "Cached geocoder requires :geocoder" {:opts opts})))
  (when-not cache
    (throw (ex-info "Cached geocoder requires :cache" {:opts opts})))
  (->CachedGeocoder (or id :geocoder)
                    geocoder
                    cache
                    (long (or hit-ttl-ms default-hit-ttl-ms))
                    (long (or empty-ttl-ms default-empty-ttl-ms))))
