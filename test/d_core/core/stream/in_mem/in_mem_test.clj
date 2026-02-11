(ns d-core.core.stream.in-mem.in-mem-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.stream.protocol :as p]
            [d-core.core.stream.in-mem.in-mem :as in-mem]
            [integrant.core :as ig]))

(defn- create-backend []
  (ig/init-key :core-service.app.streams.in-memory/backend {}))

(deftest basic-operations-test
  (let [backend (create-backend)
        stream "test-stream"]
    (testing "append and read forward"
      (let [id1 (p/append-payload! backend stream "payload1")
            _ (p/append-payload! backend stream "payload2")
            res (p/read-payloads backend stream {:direction :forward})]
        (is (= 2 (count (:entries res))))
        (is (= "payload1" (:payload (first (:entries res)))))
        (is (= id1 (:id (first (:entries res)))))))

    (testing "append batch"
      (p/append-batch! backend stream ["p3" "p4"])
      (let [all (p/read-payloads backend stream {:direction :forward})]
        (is (= 4 (count (:entries all))))
        (is (= ["payload1" "payload2" "p3" "p4"] (map :payload (:entries all))))))

    (testing "read backward"
      (let [res (p/read-payloads backend stream {:direction :backward :limit 2})]
        (is (= 2 (count (:entries res))))
        (is (= ["p4" "p3"] (map :payload (:entries res))))))

    (testing "cursor and limit"
      (let [res1 (p/read-payloads backend stream {:direction :forward :limit 2})
            cursor (:next-cursor res1)
            res2 (p/read-payloads backend stream {:direction :forward :cursor cursor :limit 2})]
        (is (= 2 (count (:entries res1))))
        (is (= 2 (count (:entries res2))))
        (is (= ["payload1" "payload2"] (map :payload (:entries res1))))
        (is (= ["p3" "p4"] (map :payload (:entries res2))))))))

(deftest blocking-read-test
  (let [backend (create-backend)
        stream "async-stream"]
    (testing "blocking read times out"
      (let [start (System/currentTimeMillis)
            res (p/read-payloads backend stream {:direction :forward :timeout 100})]
        (is (empty? (:entries res)))
        (is (>= (- (System/currentTimeMillis) start) 100))))

    (testing "blocking read succeeds when data arrives"
      (let [fut (future (p/read-payloads backend stream {:direction :forward :timeout 2000}))]
        (Thread/sleep 50)
        (p/append-payload! backend stream "late-arrival")
        (let [res @fut]
          (is (= 1 (count (:entries res))))
          (is (= "late-arrival" (:payload (first (:entries res))))))))

    (testing "read rejects missing direction"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"requires :direction"
                            (p/read-payloads backend stream {:timeout 100}))))))

(deftest trim-test
  (let [backend (create-backend)
        stream "trim-stream"]
    (p/append-payload! backend stream "p1")
    (let [id2 (p/append-payload! backend stream "p2")]
      (p/append-payload! backend stream "p3")
      (p/trim-stream! backend stream id2)
      (let [res (p/read-payloads backend stream {:direction :forward})]
        (is (= 1 (count (:entries res))))
        (is (= "p3" (:payload (first (:entries res)))))))))

(deftest utilities-test
  (let [backend (create-backend)]
    (testing "cursors"
      (p/set-cursor! backend "c1" "val1")
      (is (= "val1" (p/get-cursor backend "c1"))))
    
    (testing "next-sequence"
      (is (= 1 (p/next-sequence! backend "s1")))
      (is (= 2 (p/next-sequence! backend "s1"))))

    (testing "list-streams"
      (p/append-payload! backend "user:1:events" "e1")
      (p/append-payload! backend "user:2:events" "e2")
      (p/append-payload! backend "admin:events" "e3")
      (is (= 2 (count (p/list-streams backend "user:*")))))))
