(ns d-core.core.idempotency.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.idempotency.protocol :as p]
            [d-core.core.idempotency.redis :as redis]))

(deftest claim-returns-completed-when-result-exists
  (testing "claim returns completed when lua check returns completed payload"
    (let [idempotency (redis/->RedisIdempotency :redis "dcore:idemp:" 1000)]
      (with-redefs [d-core.core.idempotency.redis/claim-or-read-result! (fn [_ result-k pending-k ttl-ms]
                                                                          (when (and (= result-k "dcore:idemp:k1:result")
                                                                                     (= pending-k "dcore:idemp:k1:pending")
                                                                                     (= ttl-ms 5000))
                                                                            [2 (pr-str {:message-id "m1"})]))]
        (is (= {:ok true :status :completed :response {:message-id "m1"}}
               (p/claim! idempotency "k1" 5000)))))))

(deftest claim-returns-claimed-on-first-acquire
  (testing "claim returns claimed when lua check returns acquired"
    (let [idempotency (redis/->RedisIdempotency :redis "dcore:idemp:" 1000)
          calls (atom [])]
      (with-redefs [d-core.core.idempotency.redis/claim-or-read-result! (fn [_ result-k pending-k ttl-ms]
                                                                          (swap! calls conj {:result-key result-k :pending-key pending-k :ttl-ms ttl-ms})
                                                                          [1])]
        (is (= {:ok true :status :claimed}
               (p/claim! idempotency "k2" 7000)))
        (is (= [{:result-key "dcore:idemp:k2:result"
                 :pending-key "dcore:idemp:k2:pending"
                 :ttl-ms 7000}]
               @calls))))))

(deftest claim-returns-in-progress-when-already-claimed
  (testing "claim returns in-progress when lua check returns in-progress"
    (let [idempotency (redis/->RedisIdempotency :redis "dcore:idemp:" 1000)]
      (with-redefs [d-core.core.idempotency.redis/claim-or-read-result! (fn [_ _ _ _] [0])]
        (is (= {:ok true :status :in-progress}
               (p/claim! idempotency "k3" 2000)))))))

(deftest complete-writes-result-and-clears-pending
  (testing "complete stores encoded response and removes pending key"
    (let [idempotency (redis/->RedisIdempotency :redis "dcore:idemp:" 1000)
          calls (atom [])
          response {:message-id "m2" :seq 10}]
      (with-redefs [d-core.core.idempotency.redis/write-result! (fn [_ key payload ttl-ms]
                                                                  (swap! calls conj {:op :write :key key :payload payload :ttl-ms ttl-ms})
                                                                  "OK")
                    d-core.core.idempotency.redis/delete-key! (fn [_ key]
                                                                (swap! calls conj {:op :del :key key})
                                                                1)]
        (is (= {:ok true :status :completed :response response}
               (p/complete! idempotency "k4" response 3000)))
        (is (= [{:op :write :key "dcore:idemp:k4:result" :payload (pr-str response) :ttl-ms 3000}
                {:op :del :key "dcore:idemp:k4:pending"}]
               @calls))))))

(deftest lookup-returns-decoded-response
  (testing "lookup decodes and returns completed response"
    (let [idempotency (redis/->RedisIdempotency :redis "dcore:idemp:" 1000)]
      (with-redefs [d-core.core.idempotency.redis/read-result (fn [_ key]
                                                                (when (= key "dcore:idemp:k5:result")
                                                                  (pr-str {:ok true :seq 42})))]
        (is (= {:ok true :status :completed :response {:ok true :seq 42}}
               (p/lookup idempotency "k5")))))))
