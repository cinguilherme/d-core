(ns d-core.core.clients.redis.utils-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.redis.utils :as redis-utils]))

(deftest normalize-key-test
  (testing "normalizes string, keyword, bytes and fallback types"
    (is (= "abc" (redis-utils/normalize-key "abc")))
    (is (= "abc" (redis-utils/normalize-key :abc)))
    (is (= "abc" (redis-utils/normalize-key (.getBytes "abc" "UTF-8"))))
    (is (= "123" (redis-utils/normalize-key 123)))))

(deftest fields->map-test
  (testing "returns map unchanged"
    (is (= {"k" "v"} (redis-utils/fields->map {"k" "v"}))))

  (testing "converts even sequential kv values to map and normalizes keys"
    (is (= {"a" 1 "b" 2}
           (redis-utils/fields->map [:a 1 (.getBytes "b" "UTF-8") 2]))))

  (testing "returns empty map for odd sequential values"
    (is (= {} (redis-utils/fields->map [:a 1 :b]))))

  (testing "returns empty map for unsupported input"
    (is (= {} (redis-utils/fields->map nil)))
    (is (= {} (redis-utils/fields->map 123)))))
