(ns d-core.integration.local-file-cache-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.cache.protocol :as p])
  (:import (java.nio.file Files)
           (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_LOCAL_FILE"))))

(defn- temp-dir
  []
  (-> (Files/createTempDirectory "d-core-local-file-integration" (make-array java.nio.file.attribute.FileAttribute 0))
      (.toFile)))

(defn- delete-recursive!
  [^java.io.File dir]
  (when (.exists dir)
    (doseq [^java.io.File f (reverse (file-seq dir))]
      (io/delete-file f true))))

(deftest local-file-cache-integration
  (testing "Local file cache works via Integrant wiring"
    (if-not (integration-enabled?)
      (is true "Skipping local-file cache integration test; set INTEGRATION=1")
      (let [dir (temp-dir)
            logger (:logger (h-logger/make-test-logger))
            system (ig/init {:d-core.core.cache.local-file/local-file {:root-path (.getPath dir)
                                                                        :logger logger}})
            cache (:d-core.core.cache.local-file/local-file system)
            base-key (str "dcore.int.local-file." (UUID/randomUUID))
            key1 (str base-key ":k1/with spaces")
            key2 (str base-key ":k2")]
        (try
          (p/cache-put cache key1 "v1" nil)
          (is (= "v1" (p/cache-lookup cache key1 nil)))

          (p/cache-put cache key2 (byte-array [1 2 3]) nil)
          (is (= [1 2 3] (vec (p/cache-lookup cache key2 nil))))

          (p/cache-put cache key1 "v1" {:ttl-ms 20})
          (Thread/sleep 40)
          (is (nil? (p/cache-lookup cache key1 nil)))

          (p/cache-put cache key1 "v2" nil)
          (p/cache-clear cache nil)
          (is (nil? (p/cache-lookup cache key1 nil)))
          (finally
            (ig/halt! system)
            (delete-recursive! dir)))))))
