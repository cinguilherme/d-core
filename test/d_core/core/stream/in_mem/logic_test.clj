(ns d-core.core.stream.in-mem.logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.stream.in-mem.logic :as logic]))

(deftest id-generation-test
  (testing "parse-id"
    (is (= [123 45] (logic/parse-id "123-45")))
    (is (= [0 -1] (logic/parse-id nil)))
    (is (= [0 -1] (logic/parse-id ""))))

  (testing "next-id"
    (let [now (System/currentTimeMillis)]
      (testing "same millisecond increments sequence"
        (is (= (str now "-1") (logic/next-id (str now "-0") now))))
      
      (testing "new millisecond resets sequence"
        (is (= (str (+ now 1) "-0") (logic/next-id (str now "-5") (+ now 1)))))
      
      (testing "monotonicity even if clock moves backward"
        (is (= (str now "-1") (logic/next-id (str now "-0") (- now 100))))))))

(deftest query-entries-test
  (let [entries (into (sorted-map)
                      {"1000-0" "p1"
                       "1000-1" "p2"
                       "2000-0" "p3"
                       "3000-0" "p4"})]
    (testing "forward reading"
      (let [res (logic/query-entries entries {:direction :forward :limit 2})]
        (is (= ["p1" "p2"] (map :payload (:entries res))))
        (is (= "1000-1" (:next-cursor res)))))

    (testing "forward from cursor (exclusive)"
      (let [res (logic/query-entries entries {:direction :forward :cursor "1000-1"})]
        (is (= ["p3" "p4"] (map :payload (:entries res))))))

    (testing "backward reading"
      (let [res (logic/query-entries entries {:direction :backward :limit 2})]
        (is (= ["p4" "p3"] (map :payload (:entries res))))
        (is (= "2000-0" (:next-cursor res)))))

    (testing "empty results"
      (let [res (logic/query-entries (sorted-map) {:direction :forward})]
        (is (empty? (:entries res)))
        (is (nil? (:next-cursor res)))))))

(deftest state-update-test
  (let [initial-state {:streams {} :last-ids {}}
        now 1000
        state (logic/append-entries-state initial-state "s1" ["a" "b"] now)]
    (is (= ["1000-0" "1000-1"] (keys (get-in state [:streams "s1"]))))
    (is (= ["a" "b"] (vals (get-in state [:streams "s1"]))))
    (is (= "1000-1" (get-in state [:last-ids "s1"])))
    
    (testing "appended batch respects last-id"
      (let [state2 (logic/append-entries-state state "s1" ["c"] now)]
        (is (= "1000-2" (get-in state2 [:last-ids "s1"])))
        (is (= "c" (get-in state2 [:streams "s1" "1000-2"])))))))
