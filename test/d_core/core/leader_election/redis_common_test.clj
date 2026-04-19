(ns d-core.core.leader-election.redis-common-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.redis-common :as redis-common]))

(deftest prefix-and-key-helpers
  (testing "prefix defaults and custom values are normalized to strings"
    (is (= redis-common/default-prefix (redis-common/normalize-prefix nil)))
    (is (= "custom:" (redis-common/normalize-prefix "custom:")))
    (is (= "123" (redis-common/normalize-prefix 123))))

  (testing "blank prefix is rejected"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"prefix must not be blank"
         (redis-common/normalize-prefix ""))))

  (testing "redis-like lease keys use the configured prefix"
    (is (= "prefix:orders:lease"
           (redis-common/lease-key "prefix:" "orders")))
    (is (= "prefix:orders:fencing"
           (redis-common/fencing-key "prefix:" "orders")))))
