(ns d-core.core.rate-limit.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.rate-limit.protocol :as p]
            [d-core.core.rate-limit.redis :as rl]))

(deftest consume-allowed
  (testing "consume! allows and computes remaining"
    (let [limiter (rl/->RedisRateLimiter :redis "dcore:rate-limit:" 5 1000 (constantly 1500))
          calls (atom [])]
      (with-redefs [d-core.core.rate-limit.redis/increment-window! (fn [_ redis-key amount ttl-ms]
                                                                     (swap! calls conj {:key redis-key
                                                                                        :amount amount
                                                                                        :ttl-ms ttl-ms})
                                                                     3)]
        (let [res (p/consume! limiter "api-key:1" {})]
          (is (= true (:allowed? res)))
          (is (= 2 (:remaining res)))
          (is (= 2000 (:reset-at res)))
          (is (nil? (:retry-after-ms res)))
          (is (= [{:key "dcore:rate-limit:api-key:1:1000"
                   :amount 1
                   :ttl-ms 500}]
                 @calls)))))))

(deftest consume-denied
  (testing "consume! denies when counter passes limit"
    (let [limiter (rl/->RedisRateLimiter :redis "dcore:rate-limit:" 2 1000 (constantly 1500))]
      (with-redefs [d-core.core.rate-limit.redis/increment-window! (fn [_ _ _ _] 3)]
        (let [res (p/consume! limiter "api-key:1" {})]
          (is (= false (:allowed? res)))
          (is (= 0 (:remaining res)))
          (is (= 500 (:retry-after-ms res))))))))

(deftest consume-denied-when-amount-greater-than-limit
  (testing "consume! fast-fails when amount is greater than limit"
    (let [limiter (rl/->RedisRateLimiter :redis "dcore:rate-limit:" 2 1000 (constantly 1500))
          called? (atom false)]
      (with-redefs [d-core.core.rate-limit.redis/increment-window! (fn [& _]
                                                                     (reset! called? true)
                                                                     1)]
        (let [res (p/consume! limiter "api-key:1" {:amount 3})]
          (is (= false (:allowed? res)))
          (is (= 0 (:remaining res)))
          (is (false? @called?)))))))
