(ns d-core.core.cache.in-memory-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.in-memory :as cache]
            [d-core.core.cache.protocol :as p]))

(deftest in-memory-cache-basic-ops
  (testing "In-memory cache supports lookup, put, delete, clear"
    (let [data (atom {})
          c (cache/->InMemoryCache data nil)]
      (is (nil? (p/cache-lookup c :k nil)))
      (is (= :v (p/cache-put c :k :v nil)))
      (is (= :v (p/cache-lookup c :k nil)))
      (is (nil? (p/cache-delete c :k nil)))
      (is (nil? (p/cache-lookup c :k nil)))
      (p/cache-put c :k1 :v1 nil)
      (p/cache-put c :k2 :v2 nil)
      (is (= :v2 (p/cache-lookup c :k2 nil)))
      (is (nil? (p/cache-clear c nil)))
      (is (empty? @data)))))
