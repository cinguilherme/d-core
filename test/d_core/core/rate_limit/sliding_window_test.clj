(ns d-core.core.rate-limit.sliding-window-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.rate-limit.protocol :as p]
            [d-core.core.rate-limit.sliding-window :as sw]))

(deftest sliding-window-basic
  (testing "Sliding window enforces limit within window and resets after"
    (let [now (atom 0)
          clock #(long @now)
          limiter (sw/->SlidingWindowRateLimiter 2 1000 clock (atom {}))]
      (is (:allowed? (p/consume! limiter "k" {})))
      (is (:allowed? (p/consume! limiter "k" {})))
      (is (false? (:allowed? (p/consume! limiter "k" {}))))
      (swap! now + 1001)
      (is (:allowed? (p/consume! limiter "k" {}))))))

(deftest sliding-window-amounts-and-remaining
  (testing "Sliding window accounts for amount and reports remaining"
    (let [now (atom 0)
          clock #(long @now)
          limiter (sw/->SlidingWindowRateLimiter 3 1000 clock (atom {}))
          res1 (p/consume! limiter "k" {:amount 2})
          res2 (p/consume! limiter "k" {:amount 1})
          res3 (p/consume! limiter "k" {:amount 1})]
      (is (:allowed? res1))
      (is (= 1 (:remaining res1)))
      (is (:allowed? res2))
      (is (= 0 (:remaining res2)))
      (is (false? (:allowed? res3)))
      (is (= 0 (:remaining res3))))))

(deftest sliding-window-retry-after-and-reset
  (testing "Sliding window exposes retry-after and reset-at"
    (let [now (atom 0)
          clock #(long @now)
          limiter (sw/->SlidingWindowRateLimiter 1 1000 clock (atom {}))
          res1 (p/consume! limiter "k" {})
          res2 (p/consume! limiter "k" {})]
      (is (:allowed? res1))
      (is (= 1000 (:reset-at res1)))
      (is (false? (:allowed? res2)))
      (is (= 1000 (:reset-at res2)))
      (is (= 1000 (:retry-after-ms res2)))
      (swap! now + 600)
      (let [res3 (p/consume! limiter "k" {})]
        (is (false? (:allowed? res3)))
        (is (= 400 (:retry-after-ms res3)))))))

(deftest sliding-window-opt-overrides
  (testing "consume! can override limit and window-ms"
    (let [now (atom 0)
          clock #(long @now)
          limiter (sw/->SlidingWindowRateLimiter 1 1000 clock (atom {}))]
      (is (:allowed? (p/consume! limiter "k" {:limit 2 :window-ms 500})))
      (is (:allowed? (p/consume! limiter "k" {:limit 2 :window-ms 500})))
      (is (false? (:allowed? (p/consume! limiter "k" {:limit 2 :window-ms 500}))))
      (swap! now + 501)
      (is (:allowed? (p/consume! limiter "k" {:limit 2 :window-ms 500}))))))

(deftest sliding-window-per-key-isolation
  (testing "Separate keys have independent windows"
    (let [now (atom 0)
          clock #(long @now)
          limiter (sw/->SlidingWindowRateLimiter 1 1000 clock (atom {}))]
      (is (:allowed? (p/consume! limiter "a" {})))
      (is (false? (:allowed? (p/consume! limiter "a" {}))))
      (is (:allowed? (p/consume! limiter "b" {}))))))

(deftest sliding-window-amount-exceeds-limit
  (testing "Amounts over the limit are rejected without retry"
    (let [now (atom 0)
          clock #(long @now)
          limiter (sw/->SlidingWindowRateLimiter 2 1000 clock (atom {}))
          res (p/consume! limiter "k" {:amount 3})]
      (is (false? (:allowed? res)))
      (is (= 0 (:remaining res)))
      (is (nil? (:retry-after-ms res)))
      (is (nil? (:reset-at res))))))
