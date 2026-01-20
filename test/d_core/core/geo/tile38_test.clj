(ns d-core.core.geo.tile38-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.tile38.client :as tc]
            [d-core.core.geo.protocol :as p]
            [d-core.core.geo.tile38 :as geo]))

(deftest delete-expands-id-collections
  (testing "Tile38Index delete! expands sequential ids"
    (let [calls (atom [])
          index (geo/->Tile38Index {:conn {}})]
      (with-redefs [tc/del! (fn [_ key & ids]
                              (swap! calls conj [key ids])
                              {:ok true})]
        (p/delete! index "fleet" ["a" "b"] {})
        (is (= [["fleet" ["a" "b"]]] @calls))))))
