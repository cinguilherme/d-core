(ns d-core.core.cache.local-file
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [d-core.core.cache.protocol :as p])
  (:import (java.nio ByteBuffer)
           (java.nio.file Files)
           (java.util Base64)))

(defn- now-ms []
  (System/currentTimeMillis))

(defn- ttl-ms
  [{:keys [ttl ttl-ms ttl-unit]}]
  (cond
    (number? ttl-ms) ttl-ms
    (number? ttl)
    (case (or ttl-unit :sec)
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
      nil)
    :else nil))

(defn- byte-buffer->bytes
  [^ByteBuffer buf]
  (let [dup (.duplicate buf)
        arr (byte-array (.remaining dup))]
    (.get dup arr)
    arr))

(defn- key->id
  [key]
  (let [data (.getBytes (str key) "UTF-8")
        encoder (.withoutPadding (Base64/getUrlEncoder))]
    (.encodeToString encoder data)))

(defn- data-file
  [root-path key]
  (io/file root-path (str (key->id key) ".bin")))

(defn- meta-file
  [root-path key]
  (io/file root-path (str (key->id key) ".meta.edn")))

(defn- encode-value
  [value]
  (cond
    (bytes? value) {:bytes value :type :bytes}
    (instance? ByteBuffer value) {:bytes (byte-buffer->bytes value) :type :bytes}
    (string? value) {:bytes (.getBytes ^String value "UTF-8") :type :string}
    :else {:bytes (.getBytes (pr-str value) "UTF-8") :type :edn}))

(defn- decode-value
  [bytes type]
  (case type
    :string (String. ^bytes bytes "UTF-8")
    :edn (edn/read-string (String. ^bytes bytes "UTF-8"))
    bytes))

(defn- read-meta
  [^java.io.File file]
  (when (.exists file)
    (try
      (edn/read-string (slurp file))
      (catch Exception _
        nil))))

(defn- write-meta!
  [^java.io.File file meta]
  (io/make-parents file)
  (spit file (pr-str meta)))

(defn- expired?
  [meta now]
  (when-let [expires-at (:expires-at meta)]
    (<= (long expires-at) (long now))))

(defrecord LocalFileCache [root-path logger]
  p/CacheProtocol
  (cache-lookup [_ key _opts]
    (let [data (data-file root-path key)
          meta-path (meta-file root-path key)]
      (when (.exists data)
        (let [meta (read-meta meta-path)
              now (now-ms)]
          (if (and meta (expired? meta now))
            (do
              (io/delete-file data true)
              (io/delete-file meta-path true)
              nil)
            (let [bytes (Files/readAllBytes (.toPath data))
                  type (or (:type meta) :bytes)]
              (decode-value bytes type)))))))
  (cache-put [_ key value opts]
    (let [data (data-file root-path key)
          meta-path (meta-file root-path key)
          {:keys [bytes type]} (encode-value value)
          ttl (ttl-ms opts)
          expires-at (when (and ttl (pos? ttl))
                       (+ (now-ms) ttl))
          meta {:type type
                :created-at (now-ms)
                :expires-at expires-at}]
      (io/make-parents data)
      (with-open [out (io/output-stream data)]
        (.write out ^bytes bytes))
      (write-meta! meta-path meta)
      value))
  (cache-delete [_ key _opts]
    (io/delete-file (data-file root-path key) true)
    (io/delete-file (meta-file root-path key) true)
    nil)
  (cache-clear [_ _opts]
    (doseq [^java.io.File f (file-seq (io/file root-path))
            :when (.isFile f)]
      (io/delete-file f true))
    nil))

(defmethod ig/init-key :d-core.core.cache.local-file/local-file
  [_ {:keys [root-path logger] :or {root-path "cache"}}]
  (logger/log logger :info ::initializing-local-file-cache {:root-path root-path})
  (let [dir (io/file root-path)]
    (when-not (.exists dir)
      (.mkdirs dir)))
  (->LocalFileCache root-path logger))
