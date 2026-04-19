(ns d-core.core.leader-election.common-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.common :as common]
            [d-core.libs.time :as time]))

(deftest election-id-normalization
  (testing "election ids are normalized and blanks are rejected"
    (is (= "orders" (common/normalize-election-id :orders)))
    (is (= "orders" (common/normalize-election-id "orders")))
    (is (= "bytes" (common/normalize-election-id (.getBytes "bytes" "UTF-8"))))
    (is (= "123" (common/normalize-election-id 123)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"non-blank election id"
         (common/normalize-election-id nil)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"non-blank election id"
         (common/normalize-election-id "")))))

(deftest owner-id-and-token-normalization
  (testing "owner id defaults to a generated stable-per-init value"
    (let [owner-id (common/normalize-owner-id nil)]
      (is (string? owner-id))
      (is (re-find #":" owner-id))))

  (testing "blank tokens are rejected"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"non-blank token"
         (common/normalize-token "")))))

(deftest clock-and-time-helpers
  (testing "now-ms supports java.time.Clock values"
    (let [clock (time/new-clock {:type :fixed
                                 :instant {:epoch-ms 1700000000123}
                                 :zone "UTC"})]
      (is (= 1700000000123 (common/now-ms clock)))))

  (testing "now-ms supports functions that return epoch millis or instants"
    (is (= 99 (common/now-ms (fn [] 99))))
    (is (= 1700000000999
           (common/now-ms (fn [] (time/map->instant {:epoch-ms 1700000000999})))))))

(deftest lease-and-result-helpers
  (testing "lease-ms uses default and validates positive values"
    (is (= 15000 (common/lease-ms nil 15000)))
    (is (= 500 (common/lease-ms {:lease-ms 500} 15000)))
    (is (= 15000 (common/lease-ms {:lease-ms nil} 15000)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"greater than zero"
         (common/lease-ms {:lease-ms 0} 15000)))
    (doseq [invalid ["10" :x 1.5 -1]]
      (let [ex (try
                 (common/lease-ms {:lease-ms invalid} 15000)
                 nil
                 (catch clojure.lang.ExceptionInfo ex
                   ex))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (= ::common/invalid-field (:type (ex-data ex))))
        (is (= :lease-ms (:field (ex-data ex))))
        (is (= invalid (:value (ex-data ex)))))))

  (testing "require-positive-long rejects invalid default values with typed errors"
    (doseq [invalid [nil "10" :x 1.5]]
      (let [ex (try
                 (common/require-positive-long invalid :default-lease-ms)
                 nil
                 (catch clojure.lang.ExceptionInfo ex
                   ex))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (= ::common/invalid-field (:type (ex-data ex))))
        (is (= :default-lease-ms (:field (ex-data ex))))
        (is (= invalid (:value (ex-data ex)))))))

  (testing "remaining-ttl-ms is clamped at zero"
    (is (= 500 (common/remaining-ttl-ms 1500 1000)))
    (is (= 0 (common/remaining-ttl-ms 900 1000)))
    (is (nil? (common/remaining-ttl-ms nil 1000))))

  (testing "status result never includes token and parses holder metadata"
    (is (= {:ok true
            :status :held
            :backend :redis
            :election-id "orders"
            :owner-id "node-1"
            :fencing 9
            :remaining-ttl-ms 1200}
           (common/status-result :redis "orders" ["held" "node-1" "9" "1200"])))
    (is (= {:ok true
            :status :vacant
            :backend :valkey
            :election-id "orders"}
           (common/status-result :valkey "orders" ["vacant"])))))
