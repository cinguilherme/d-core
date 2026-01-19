(ns d-core.schemas-test
(:require [clojure.test :refer [deftest is testing]]
          [d-core.core.schema :as dcs]
          [d-core.core.producers.common :as common-producer]
          [d-core.core.producers.protocol :as p]))

(deftest schema-apply-strictness
  (testing "apply-strictness propagates :closed to nested :map schemas"
    (let [schema [:map
                  [:a :int]
                  [:b [:map
                       [:c :int]]]]
          strict (dcs/apply-strictness schema :strict)
          lax (dcs/apply-strictness schema :lax)]
      (is (= true (get-in strict [1 :closed])))
      (is (= true (get-in strict [3 1 1 :closed])))
      (is (= false (get-in lax [1 :closed])))
      (is (= false (get-in lax [3 1 1 :closed]))))))

(deftest schema-validate!-ex-data
  (testing "validate! throws typed non-retriable ex-info with explain payload"
    (let [schema [:map {:closed true}
                  [:a :int]]]
      (try
        (dcs/validate! schema {:a 1 :extra "x"} {:schema-id :t :strictness :strict})
        (is false "Expected validate! to throw")
        (catch clojure.lang.ExceptionInfo e
          (let [d (ex-data e)]
            (is (= :schema-invalid (:failure/type d)))
            (is (= false (:retriable? d)))
            (is (= :t (:schema/id d)))
            (is (map? (:schema/explain d)))))))))

(deftest common-producer-enforces-topic-schema
  (testing "CommonProducer validates msg-map against topic canonical schema before delegating"
    (let [calls (atom [])
          delegate (reify p/Producer
                     (produce! [_ msg-map options]
                       (swap! calls conj {:msg msg-map :options options})
                       {:ok true}))
          routing {:topics {:t {:schema {:canonical [:map {:closed true} [:a :int]]
                                         :strictness :strict
                                         :id :t}}}
                   :publish {:t {:targets [{:producer :in-memory}]}}}
          producer (common-producer/->CommonProducer :in-memory {:in-memory delegate} routing nil nil)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (p/produce! producer {:a 1 :extra "x"} {:topic :t})))
      (is (= [] @calls))
      (is (= {:ok true}
             (select-keys (p/produce! producer {:a 1} {:topic :t}) [:ok])))
      (is (= 1 (count @calls))))))
