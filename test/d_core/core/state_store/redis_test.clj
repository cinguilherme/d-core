(ns d-core.core.state-store.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.state-store.protocol :as p]
            [d-core.core.state-store.redis :as redis]))

(deftest put-field-test
  (testing "put-field writes hash field and applies ttl when present"
    (let [store (redis/->RedisStateStore :redis)
          calls (atom [])]
      (with-redefs [d-core.core.state-store.redis/hset! (fn [_ key field value]
                                                          (swap! calls conj {:op :hset :key key :field field :value value})
                                                          1)
                    d-core.core.state-store.redis/pexpire! (fn [_ key ttl-ms]
                                                             (swap! calls conj {:op :pexpire :key key :ttl-ms ttl-ms})
                                                             1)]
        (is (= 1 (p/put-field! store "k" "f" "v" {:ttl-ms 5000})))
        (is (= [{:op :hset :key "k" :field "f" :value "v"}
                {:op :pexpire :key "k" :ttl-ms 5000}]
               @calls))))))

(deftest put-fields-test
  (testing "put-fields writes multiple fields and optional ttl"
    (let [store (redis/->RedisStateStore :redis)
          calls (atom [])]
      (with-redefs [d-core.core.state-store.redis/hset-many! (fn [_ key field->value]
                                                               (swap! calls conj {:op :hset-many :key key :field->value field->value})
                                                               2)
                    d-core.core.state-store.redis/pexpire! (fn [_ key ttl-ms]
                                                             (swap! calls conj {:op :pexpire :key key :ttl-ms ttl-ms})
                                                             1)]
        (is (= 2 (p/put-fields! store "k" {"a" "1" "b" "2"} {:ttl-ms 3000})))
        (is (= [{:op :hset-many :key "k" :field->value {"a" "1" "b" "2"}}
                {:op :pexpire :key "k" :ttl-ms 3000}]
               @calls))))))

(deftest get-operations-test
  (testing "get-field delegates and get-all normalizes map"
    (let [store (redis/->RedisStateStore :redis)]
      (with-redefs [d-core.core.state-store.redis/hget (fn [_ _ _] "11")
                    d-core.core.state-store.redis/hgetall (fn [_ _] [:a "11" :b "22"])]
        (is (= "11" (p/get-field store "k" "a" nil)))
        (is (= {"a" "11" "b" "22"} (p/get-all store "k" nil)))))))

(deftest delete-and-expire-test
  (testing "delete-fields and expire delegate"
    (let [store (redis/->RedisStateStore :redis)]
      (with-redefs [d-core.core.state-store.redis/hdel! (fn [_ key fields]
                                                          (when (and (= key "k") (= fields ["a" "b"]))
                                                            2))
                    d-core.core.state-store.redis/pexpire! (fn [_ key ttl-ms]
                                                             (when (and (= key "k") (= ttl-ms 9000))
                                                               1))]
        (is (= 2 (p/delete-fields! store "k" ["a" "b"] nil)))
        (is (= 1 (p/expire! store "k" 9000 nil)))))))

(deftest set-max-field-test
  (testing "set-max-field returns true and passes ttl in single eval operation"
    (let [store (redis/->RedisStateStore :redis)
          calls (atom [])]
      (with-redefs [d-core.core.state-store.redis/eval-set-max! (fn [_ key field value ttl-ms]
                                                                  (swap! calls conj {:op :max :key key :field field :value value :ttl-ms ttl-ms})
                                                                  1)]
        (is (true? (p/set-max-field! store "k" "user-1" 42 {:ttl-ms 1000})))
        (is (= [{:op :max :key "k" :field "user-1" :value "42" :ttl-ms 1000}]
               @calls))))))

(deftest set-max-field-no-update-still-passes-ttl
  (testing "set-max-field returns false when not updated and still includes ttl in eval call"
    (let [store (redis/->RedisStateStore :redis)
          calls (atom [])]
      (with-redefs [d-core.core.state-store.redis/eval-set-max! (fn [_ key field value ttl-ms]
                                                                  (swap! calls conj {:op :max :key key :field field :value value :ttl-ms ttl-ms})
                                                                  0)]
        (is (false? (p/set-max-field! store "k" "user-1" 42 {:ttl-ms 2000})))
        (is (= [{:op :max :key "k" :field "user-1" :value "42" :ttl-ms 2000}]
               @calls))))))

(deftest parse-numeric-value-test
  (testing "accepts numeric values and numeric strings"
    (is (= "42" (redis/parse-numeric-value 42)))
    (is (= "42.5" (redis/parse-numeric-value 42.5)))
    (is (= "-9" (redis/parse-numeric-value "-9")))
    (is (= "10.25" (redis/parse-numeric-value "10.25"))))

  (testing "rejects non-numeric values"
    (is (nil? (redis/parse-numeric-value nil)))
    (is (nil? (redis/parse-numeric-value "abc")))
    (is (nil? (redis/parse-numeric-value "1a")))
    (is (nil? (redis/parse-numeric-value :x)))))

(deftest set-max-field-invalid-input
  (testing "set-max-field throws for non-numeric input before eval"
    (let [store (redis/->RedisStateStore :redis)
          called? (atom false)]
      (with-redefs [d-core.core.state-store.redis/eval-set-max! (fn [& _]
                                                                  (reset! called? true)
                                                                  1)]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"state-store set-max requires numeric value"
             (p/set-max-field! store "k" "f" "abc" {:ttl-ms 1000})))
        (is (false? @called?))))))

(deftest sorted-set-operations-test
  (testing "zadd and zcount delegate"
    (let [store (redis/->RedisStateStore :redis)]
      (with-redefs [d-core.core.state-store.redis/zadd-score! (fn [_ key score member]
                                                                (when (and (= key "idx") (= score 10) (= member "m1"))
                                                                  1))
                    d-core.core.state-store.redis/zcount-range (fn [_ key min-score max-score]
                                                                 (when (and (= key "idx") (= min-score "(8") (= max-score "+inf"))
                                                                   3))]
        (is (= 1 (p/zadd! store "idx" 10 "m1" nil)))
        (is (= 3 (p/zcount store "idx" "(8" "+inf" nil)))))))
