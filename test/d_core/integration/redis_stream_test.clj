(ns d-core.integration.redis-stream-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.redis.client :as redis-client]
            [d-core.core.stream.protocol :as p]
            [d-core.core.stream.redis.logic :as logic]
            [d-core.core.stream.redis.redis :as redis-stream]
            [taoensso.carmine :as car])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_REDIS_STREAM"))))

(defn- redis-uri
  []
  (or (System/getenv "DCORE_REDIS_URI")
      "redis://localhost:6379"))

(defn- wait-for
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 10) (recur))
          false)))))

(defn- del-key!
  [client key]
  (car/wcar (:conn client)
    (car/del key)))

(defn- cleanup!
  [client stream-keys meta-prefix]
  (doseq [k (concat stream-keys
                    [(logic/cursor-hash-key meta-prefix)
                     (logic/sequence-hash-key meta-prefix)])
          :when k]
    (del-key! client k)))

(deftest redis-stream-backend-basic-ops
  (testing "append, batch append, directional read, cursor paging"
    (if-not (integration-enabled?)
      (is true "Skipping Redis stream integration test; set INTEGRATION=1")
      (let [client (redis-client/make-client {:uri (redis-uri)})
            base (str "dcore:int:stream:" (UUID/randomUUID))
            stream (str base ":main")
            meta-prefix (str "__dcore:int:meta:" (UUID/randomUUID))
            backend (redis-stream/->RedisStreamBackend client meta-prefix 100)]
        (try
          (let [id1 (p/append-payload! backend stream "payload1")
                _ (p/append-payload! backend stream "payload2")
                read1 (p/read-payloads backend stream {:direction :forward})]
            (is (= 2 (count (:entries read1))))
            (is (= "payload1" (:payload (first (:entries read1)))))
            (is (= id1 (:id (first (:entries read1))))))

          (p/append-batch! backend stream ["p3" "p4"])
          (let [all (p/read-payloads backend stream {:direction :forward})]
            (is (= 4 (count (:entries all))))
            (is (= ["payload1" "payload2" "p3" "p4"]
                   (map :payload (:entries all)))))

          (let [backward (p/read-payloads backend stream {:direction :backward :limit 2})]
            (is (= 2 (count (:entries backward))))
            (is (= ["p4" "p3"] (map :payload (:entries backward)))))

          (let [res1 (p/read-payloads backend stream {:direction :forward :limit 2})
                cursor (:next-cursor res1)
                res2 (p/read-payloads backend stream {:direction :forward :cursor cursor :limit 2})]
            (is (= 2 (count (:entries res1))))
            (is (= 2 (count (:entries res2))))
            (is (= ["payload1" "payload2"] (map :payload (:entries res1))))
            (is (= ["p3" "p4"] (map :payload (:entries res2)))))
          (finally
            (cleanup! client [stream] meta-prefix)))))))

(deftest redis-stream-backend-blocking-read
  (testing "blocking reads timeout and wake up when data arrives"
    (if-not (integration-enabled?)
      (is true "Skipping Redis stream integration test; set INTEGRATION=1")
      (let [client (redis-client/make-client {:uri (redis-uri)})
            stream (str "dcore:int:stream:async:" (UUID/randomUUID))
            stream-zero (str "dcore:int:stream:async-zero:" (UUID/randomUUID))
            meta-prefix (str "__dcore:int:meta:" (UUID/randomUUID))
            backend (redis-stream/->RedisStreamBackend client meta-prefix 100)]
        (try
          (let [start (System/currentTimeMillis)
                res (p/read-payloads backend stream {:timeout 100})]
            (is (empty? (:entries res)))
            (is (>= (- (System/currentTimeMillis) start) 100)))

          (let [result (promise)]
            (future
              (deliver result (p/read-payloads backend stream {:timeout 2000})))
            (Thread/sleep 50)
            (p/append-payload! backend stream "late-arrival")
            (is (wait-for #(realized? result) 1000))
            (let [res @result]
              (is (= 1 (count (:entries res))))
              (is (= "late-arrival" (:payload (first (:entries res)))))))

          (let [result-zero (promise)]
            (future
              (deliver result-zero (p/read-payloads backend stream-zero {:timeout 0})))
            (Thread/sleep 50)
            (p/append-payload! backend stream-zero "late-arrival-zero")
            (is (wait-for #(realized? result-zero) 1000))
            (let [res @result-zero]
              (is (= 1 (count (:entries res))))
              (is (= "late-arrival-zero" (:payload (first (:entries res)))))))
          (finally
            (cleanup! client [stream stream-zero] meta-prefix)))))))

(deftest redis-stream-backend-utilities-and-trim
  (testing "trim, cursors, sequence, and stream listing"
    (if-not (integration-enabled?)
      (is true "Skipping Redis stream integration test; set INTEGRATION=1")
      (let [client (redis-client/make-client {:uri (redis-uri)})
            base (str "dcore:int:stream:util:" (UUID/randomUUID))
            stream (str base ":trim")
            user-stream-1 (str base ":user:1:events")
            user-stream-2 (str base ":user:2:events")
            admin-stream (str base ":admin:events")
            meta-prefix (str "__dcore:int:meta:" (UUID/randomUUID))
            backend (redis-stream/->RedisStreamBackend client meta-prefix 100)]
        (try
          (p/append-payload! backend stream "p1")
          (let [id2 (p/append-payload! backend stream "p2")]
            (p/append-payload! backend stream "p3")
            (p/trim-stream! backend stream id2)
            (let [res (p/read-payloads backend stream {:direction :forward})]
              (is (= 1 (count (:entries res))))
              (is (= "p3" (:payload (first (:entries res)))))))

          (p/set-cursor! backend "c1" "val1")
          (is (= "val1" (p/get-cursor backend "c1")))

          (is (= 1 (p/next-sequence! backend "s1")))
          (is (= 2 (p/next-sequence! backend "s1")))

          (p/append-payload! backend user-stream-1 "e1")
          (p/append-payload! backend user-stream-2 "e2")
          (p/append-payload! backend admin-stream "e3")
          (let [matches (p/list-streams backend (str base ":user:*"))]
            (is (= 2 (count matches)))
            (is (some #(= user-stream-1 %) matches))
            (is (some #(= user-stream-2 %) matches)))
          (finally
            (cleanup! client [stream user-stream-1 user-stream-2 admin-stream] meta-prefix)))))))
