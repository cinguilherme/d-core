(ns d-core.core.stream.redis.logic-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.stream.redis.logic :as logic]))

(deftest directions-and-ranges-test
  (testing "direction and cursor normalization"
    (is (= :forward (logic/normalize-direction nil)))
    (is (= :forward (logic/normalize-direction :forward)))
    (is (= :backward (logic/normalize-direction :backward)))
    (is (= "-" (logic/range-start nil)))
    (is (= "(10-0" (logic/range-start "10-0")))
    (is (= "+" (logic/revrange-end nil)))
    (is (= "(10-0" (logic/revrange-end "10-0")))))

(deftest response-parsing-test
  (testing "range response becomes protocol entries"
    (let [response [["1000-0" ["payload" "a"]]
                    ["1000-1" ["payload" "b"]]
                    ["1000-2" ["other" "value" "payload" "c"]]]
          entries (logic/range-response->entries response)]
      (is (= [{:id "1000-0" :payload "a"}
              {:id "1000-1" :payload "b"}
              {:id "1000-2" :payload "c"}]
             entries))))

  (testing "xread response flattens per stream"
    (let [response [["stream-1" [["1000-0" ["payload" "a"]]
                                 ["1000-1" ["payload" "b"]]]]]
          entries (logic/xread-response->entries response)]
      (is (= [{:id "1000-0" :payload "a"}
              {:id "1000-1" :payload "b"}]
             entries)))))

(deftest cursor-and-limit-test
  (testing "next cursor only exists on full page"
    (is (= "1000-1"
           (logic/next-cursor [{:id "1000-0"} {:id "1000-1"}] 2)))
    (is (nil? (logic/next-cursor [{:id "1000-0"}] 2)))
    (is (= {:entries [{:id "1000-0"}]
            :next-cursor nil}
           (logic/read-result [{:id "1000-0"}] 2)))))

(deftest blocking-and-trim-test
  (testing "blocking request predicate"
    (is (true? (logic/blocking-request?
                {:direction :forward :timeout 100}
                {:entries []})))
    (is (false? (logic/blocking-request?
                 {:direction :backward :timeout 100}
                 {:entries []})))
    (is (false? (logic/blocking-request?
                 {:direction :forward :timeout nil}
                 {:entries []})))
    (is (false? (logic/blocking-request?
                 {:direction :forward :timeout 100}
                 {:entries [{:id "1-0"}]}))))

  (testing "trim uses next redis id to preserve strict > semantics"
    (is (= [1000 0] (logic/parse-id "1000-0")))
    (is (nil? (logic/parse-id "bad-id")))
    (is (= "1000-1" (logic/next-stream-id "1000-0")))
    (is (= "1000-1" (logic/trim-min-id "1000-0")))
    (is (= "bad-id" (logic/trim-min-id "bad-id")))))

(deftest metadata-and-scan-test
  (testing "metadata keys are derived from a configurable prefix"
    (is (= "__dcore:stream:cursors" (logic/cursor-hash-key nil)))
    (is (= "custom:sequences" (logic/sequence-hash-key "custom")))
    (is (= "custom:cursors" (logic/cursor-hash-key "custom"))))

  (testing "scan response parsing"
    (is (= {:next-cursor "42" :keys ["a" "b"]}
           (logic/parse-scan-response ["42" ["a" "b"]])))
    (is (= {:next-cursor "0" :keys []}
           (logic/parse-scan-response nil)))
    (is (true? (logic/continue-scan? "1")))
    (is (false? (logic/continue-scan? "0")))))
