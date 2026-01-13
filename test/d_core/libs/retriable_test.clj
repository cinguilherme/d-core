(ns d-core.libs.retriable-test
  (:require [clojure.test :refer [deftest is]]
            [d-core.libs.retriable :refer [retriable]]))

(deftest retriable-no-retry-on-success
  (let [attempts (atom 0)
        backoff-args (atom [])
        backoff (fn [n] (swap! backoff-args conj n) 0)]
    (is (= :ok
           (retriable 5 backoff [RuntimeException]
             (swap! attempts inc)
             :ok)))
    (is (= 1 @attempts))
    (is (= [] @backoff-args))))

(deftest retriable-retries-only-on-listed-exceptions
  (let [attempts (atom 0)
        backoff-args (atom [])
        backoff (fn [n] (swap! backoff-args conj n) 0)]
    (is (= :ok
           (retriable 5 backoff [RuntimeException]
             (swap! attempts inc)
             (if (< @attempts 3)
               (throw (RuntimeException. "fail"))
               :ok))))
    (is (= 3 @attempts))
    ;; two failures -> backoff called with 1-based failure counts
    (is (= [1 2] @backoff-args))))

(deftest retriable-does-not-retry-on-non-matching-exception
  (let [attempts (atom 0)
        backoff-args (atom [])
        backoff (fn [n] (swap! backoff-args conj n) 0)]
    (is (thrown? RuntimeException
                 (retriable 5 backoff [IllegalArgumentException]
                   (swap! attempts inc)
                   (throw (RuntimeException. "no retry")))))
    (is (= 1 @attempts))
    (is (= [] @backoff-args))))

(deftest retriable-stops-at-max-attempts
  (let [attempts (atom 0)
        backoff-args (atom [])
        backoff (fn [n] (swap! backoff-args conj n) 0)]
    (is (thrown? RuntimeException
                 (retriable 3 backoff [RuntimeException]
                   (swap! attempts inc)
                   (throw (RuntimeException. "always fails")))))
    ;; total attempts includes the initial one
    (is (= 3 @attempts))
    ;; backoff happens only before retries (after failures 1 and 2)
    (is (= [1 2] @backoff-args))))

(deftest retriable-matches-subclasses-via-instance
  (let [attempts (atom 0)
        backoff (constantly 0)]
    (is (= :ok
           (retriable 3 backoff [RuntimeException]
             (swap! attempts inc)
             (if (< @attempts 2)
               (throw (ex-info "subclass of RuntimeException" {:attempt @attempts}))
               :ok))))))

