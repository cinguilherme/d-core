(ns d-core.core.rate-limit.leaky-bucket-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.rate-limit.protocol :as p]
            [d-core.core.rate-limit.leaky-bucket :as lb]))

(deftest leaky-bucket-basic
  (testing "Leaky bucket drains over time and enforces capacity"
    (let [now (atom 0)
          clock #(long @now)
          limiter (lb/->LeakyBucketRateLimiter 10.0 0.01 clock (atom {}))]
      (is (:allowed? (p/consume! limiter "k" {:amount 5})))
      (swap! now + 200)
      (is (:allowed? (p/consume! limiter "k" {:amount 6})))
      (let [res (p/consume! limiter "k" {:amount 2})]
        (is (false? (:allowed? res)))
        (is (= 100 (:retry-after-ms res)))
        (is (= 1 (:remaining res)))))))

(deftest leaky-bucket-reset-at
  (testing "reset-at reflects current bucket level"
    (let [now (atom 0)
          clock #(long @now)
          limiter (lb/->LeakyBucketRateLimiter 10.0 0.01 clock (atom {}))
          res1 (p/consume! limiter "k" {:amount 4})]
      (is (:allowed? res1))
      (is (= 400 (:reset-at res1)))
      (swap! now + 100)
      (let [res2 (p/consume! limiter "k" {:amount 1})]
        (is (:allowed? res2))
        (is (= 500 (:reset-at res2)))))))

(deftest leaky-bucket-per-key-isolation
  (testing "Separate keys have independent buckets"
    (let [now (atom 0)
          clock #(long @now)
          limiter (lb/->LeakyBucketRateLimiter 5.0 0.01 clock (atom {}))]
      (is (:allowed? (p/consume! limiter "a" {:amount 5})))
      (is (false? (:allowed? (p/consume! limiter "a" {:amount 1}))))
      (is (:allowed? (p/consume! limiter "b" {:amount 5}))))))

(deftest leaky-bucket-amount-exceeds-capacity
  (testing "Amounts over the capacity are rejected without retry"
    (let [now (atom 0)
          clock #(long @now)
          limiter (lb/->LeakyBucketRateLimiter 5.0 0.01 clock (atom {}))
          res (p/consume! limiter "k" {:amount 6})]
      (is (false? (:allowed? res)))
      (is (= 0 (:remaining res)))
      (is (nil? (:retry-after-ms res)))
      (is (nil? (:reset-at res))))))
