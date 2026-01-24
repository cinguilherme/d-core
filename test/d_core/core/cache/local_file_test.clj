(ns d-core.core.cache.local-file-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.local-file :as cache]
            [d-core.core.cache.protocol :as p])
  (:import (java.nio.file Files)))

(defn- temp-dir
  []
  (-> (Files/createTempDirectory "d-core-local-file-cache" (make-array java.nio.file.attribute.FileAttribute 0))
      (.toFile)))

(defn- delete-recursive!
  [^java.io.File dir]
  (when (.exists dir)
    (doseq [^java.io.File f (reverse (file-seq dir))]
      (io/delete-file f true))))

(deftest local-file-cache-basic-ops
  (testing "Local file cache supports lookup, put, delete, clear"
    (let [dir (temp-dir)]
      (try
        (let [c (cache/->LocalFileCache (.getPath dir) nil)]
          (is (nil? (p/cache-lookup c :k nil)))
          (is (= :v (p/cache-put c :k :v nil)))
          (is (= :v (p/cache-lookup c :k nil)))
          (is (nil? (p/cache-delete c :k nil)))
          (is (nil? (p/cache-lookup c :k nil)))
          (p/cache-put c :k1 "v1" nil)
          (p/cache-put c :k2 "v2" nil)
          (is (= "v2" (p/cache-lookup c :k2 nil)))
          (is (nil? (p/cache-clear c nil))))
        (finally
          (delete-recursive! dir))))))

(deftest local-file-cache-preserves-bytes
  (testing "Local file cache preserves byte arrays"
    (let [dir (temp-dir)]
      (try
        (let [c (cache/->LocalFileCache (.getPath dir) nil)
              payload (byte-array [1 2 3 4])]
          (p/cache-put c "b" payload nil)
          (is (= (seq payload) (seq (p/cache-lookup c "b" nil)))))
        (finally
          (delete-recursive! dir))))))

(deftest local-file-cache-ttl
  (testing "Local file cache expires entries based on ttl-ms"
    (let [dir (temp-dir)]
      (try
        (let [c (cache/->LocalFileCache (.getPath dir) nil)]
          (p/cache-put c "t" "v" {:ttl-ms 20})
          (is (= "v" (p/cache-lookup c "t" nil)))
          (Thread/sleep 40)
          (is (nil? (p/cache-lookup c "t" nil))))
        (finally
          (delete-recursive! dir))))))

