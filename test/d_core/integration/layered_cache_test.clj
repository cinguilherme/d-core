(ns d-core.integration.layered-cache-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.cache.layered :as layered]
            [d-core.core.cache.protocol :as p]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.clients.redis]
            [d-core.core.cache.redis]
            [d-core.core.cache.in-memory]
            [d-core.core.storage.minio])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_LAYERED"))))

(defn- redis-uri
  []
  (or (System/getenv "DCORE_REDIS_URI")
      "redis://localhost:6379"))

(defn- minio-endpoint
  []
  (or (System/getenv "DCORE_MINIO_ENDPOINT")
      "http://localhost:9000"))

(defn- minio-access-key
  []
  (or (System/getenv "DCORE_MINIO_ACCESS_KEY")
      "minio"))

(defn- minio-secret-key
  []
  (or (System/getenv "DCORE_MINIO_SECRET_KEY")
      "minio123"))

(defn- minio-bucket
  []
  (or (System/getenv "DCORE_MINIO_BUCKET")
      "dcore-cache-test"))

(defn- wait-for
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 20) (recur))
          false)))))

(defn- init-system
  [logger]
  (ig/init {:d-core.core.clients.redis/client {:uri (redis-uri)}
            :d-core.core.cache.redis/redis {:redis-client (ig/ref :d-core.core.clients.redis/client)}
            :d-core.core.cache.in-memory/in-memory {:logger logger}
            :d-core.core.storage/minio {:endpoint (minio-endpoint)
                                        :access-key (minio-access-key)
                                        :secret-key (minio-secret-key)
                                        :bucket (minio-bucket)
                                        :logger logger}}))

(defn- minio-source
  [minio]
  {:read-fn (fn [key opts] (storage/storage-get minio key opts))
   :write-fn (fn [key value opts] (storage/storage-put minio key value opts))
   :delete-fn (fn [key opts] (storage/storage-delete minio key opts))})

(deftest integration-layered-cache-in-memory-redis-minio
  (testing "Layered cache composes in-memory -> redis -> minio"
    (if-not (integration-enabled?)
      (is true "Skipping layered cache integration test; set INTEGRATION=1")
      (let [logger (:logger (h-logger/make-test-logger))
            system (init-system logger)
            mem (:d-core.core.cache.in-memory/in-memory system)
            redis (:d-core.core.cache.redis/redis system)
            minio (:d-core.core.storage/minio system)
            source (minio-source minio)
            cache (layered/->LayeredCache [{:id :mem
                                            :cache mem
                                            :ttl-ms 200
                                            :promote? true}
                                           {:id :redis
                                            :cache redis
                                            :ttl-ms 200}]
                                          source
                                          :write-through
                                          ::layered/miss
                                          logger)
            base-key (str "dcore.int.layered." (UUID/randomUUID))
            key1 (str base-key ":k1")]
        (try
          (p/cache-put cache key1 "v1" nil)
          (is (= "v1" (p/cache-lookup cache key1 nil)))
          (is (= "v1" (p/cache-lookup redis key1 nil)))
          (is (= "v1" (storage/storage-get minio key1 nil)))

          (Thread/sleep 1200)
          (p/cache-delete mem key1 nil) ;; simulate in-memory TTL
          (is (wait-for #(nil? (p/cache-lookup redis key1 nil)) 2000))
          (is (= "v1" (p/cache-lookup cache key1 nil)))
          (is (= "v1" (p/cache-lookup redis key1 nil)))
          (finally
            (p/cache-delete mem key1 nil)
            (p/cache-delete redis key1 nil)
            (storage/storage-delete minio key1 nil)
            (ig/halt! system)))))))

(deftest integration-layered-cache-write-around
  (testing "Write-around skips cache writes but persists to source"
    (if-not (integration-enabled?)
      (is true "Skipping layered cache integration test; set INTEGRATION=1")
      (let [logger (:logger (h-logger/make-test-logger))
            system (init-system logger)
            mem (:d-core.core.cache.in-memory/in-memory system)
            redis (:d-core.core.cache.redis/redis system)
            minio (:d-core.core.storage/minio system)
            source (minio-source minio)
            cache (layered/->LayeredCache [{:id :mem :cache mem :ttl-ms 500 :promote? true}
                                           {:id :redis :cache redis :ttl-ms 500}]
                                          source
                                          :write-around
                                          ::layered/miss
                                          logger)
            base-key (str "dcore.int.layered." (UUID/randomUUID))
            key1 (str base-key ":k1")]
        (try
          (p/cache-put cache key1 "v1" nil)
          (is (nil? (p/cache-lookup mem key1 nil)))
          (is (nil? (p/cache-lookup redis key1 nil)))
          (is (= "v1" (storage/storage-get minio key1 nil)))
          (is (= "v1" (p/cache-lookup cache key1 nil)))
          (is (= "v1" (p/cache-lookup redis key1 nil)))
          (finally
            (p/cache-delete mem key1 nil)
            (p/cache-delete redis key1 nil)
            (storage/storage-delete minio key1 nil)
            (ig/halt! system)))))))

(deftest integration-layered-cache-promotes-from-redis
  (testing "Redis hit promotes to memory without source reads"
    (if-not (integration-enabled?)
      (is true "Skipping layered cache integration test; set INTEGRATION=1")
      (let [logger (:logger (h-logger/make-test-logger))
            system (init-system logger)
            mem (:d-core.core.cache.in-memory/in-memory system)
            redis (:d-core.core.cache.redis/redis system)
            cache (layered/->LayeredCache [{:id :mem :cache mem :ttl-ms 1000 :promote? true}
                                           {:id :redis :cache redis :ttl-ms 1000}]
                                          nil
                                          :write-through
                                          ::layered/miss
                                          logger)
            base-key (str "dcore.int.layered." (UUID/randomUUID))
            key1 (str base-key ":k1")]
        (try
          (p/cache-put redis key1 "v1" nil)
          (is (nil? (p/cache-lookup mem key1 nil)))
          (is (= "v1" (p/cache-lookup cache key1 nil)))
          (is (= "v1" (p/cache-lookup mem key1 nil)))
          (finally
            (p/cache-delete mem key1 nil)
            (p/cache-delete redis key1 nil)
            (ig/halt! system)))))))

(deftest integration-layered-cache-ttl-timing-write-through-vs-write-around
  (testing "Write-through expires redis immediately; write-around expires after read-through"
    (if-not (integration-enabled?)
      (is true "Skipping layered cache integration test; set INTEGRATION=1")
      (let [logger (:logger (h-logger/make-test-logger))
            system (init-system logger)
            mem (:d-core.core.cache.in-memory/in-memory system)
            redis (:d-core.core.cache.redis/redis system)
            minio (:d-core.core.storage/minio system)
            source (minio-source minio)
            ttl-ms 200
            cache-write-through (layered/->LayeredCache [{:id :mem :cache mem :ttl-ms ttl-ms :promote? true}
                                                         {:id :redis :cache redis :ttl-ms ttl-ms}]
                                                        source
                                                        :write-through
                                                        ::layered/miss
                                                        logger)
            cache-write-around (layered/->LayeredCache [{:id :mem :cache mem :ttl-ms ttl-ms :promote? true}
                                                        {:id :redis :cache redis :ttl-ms ttl-ms}]
                                                       source
                                                       :write-around
                                                       ::layered/miss
                                                       logger)
            base-key (str "dcore.int.layered." (UUID/randomUUID))
            key-through (str base-key ":through")
            key-around (str base-key ":around")]
        (try
          ;; write-through: redis TTL starts immediately
          (p/cache-put cache-write-through key-through "v-through" nil)
          (is (= "v-through" (p/cache-lookup redis key-through nil)))
          (Thread/sleep 600)
          (is (wait-for #(nil? (p/cache-lookup redis key-through nil)) 2000))
          (is (= "v-through" (storage/storage-get minio key-through nil)))

          ;; write-around: no cache write; redis remains empty until read-through
          (p/cache-put cache-write-around key-around "v-around" nil)
          (Thread/sleep 600)
          (is (nil? (p/cache-lookup redis key-around nil)))
          (is (= "v-around" (storage/storage-get minio key-around nil)))
          (is (= "v-around" (p/cache-lookup cache-write-around key-around nil)))
          (is (= "v-around" (p/cache-lookup redis key-around nil)))
          (Thread/sleep 600)
          (is (wait-for #(nil? (p/cache-lookup redis key-around nil)) 2000))
          (finally
            (p/cache-delete mem key-through nil)
            (p/cache-delete mem key-around nil)
            (p/cache-delete redis key-through nil)
            (p/cache-delete redis key-around nil)
            (storage/storage-delete minio key-through nil)
            (storage/storage-delete minio key-around nil)
            (ig/halt! system)))))))

(deftest integration-layered-cache-write-back-pending
  (testing "Write-back strategy pending implementation"
    (is true "Pending: add write-back integration test once strategy is implemented")))
