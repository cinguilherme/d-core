(ns d-core.core.messaging.routing-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.messaging.routing :as routing]))

(deftest source-for-topic-resolution
  (testing "uses explicit :source when present"
    (let [cfg {:topics {:orders {:source :kafka}}
               :defaults {:source :redis}}]
      (is (= :kafka (routing/source-for-topic cfg :orders)))))
  (testing "uses first :sources entry when :source absent"
    (let [cfg {:topics {:orders {:sources [:jetstream :kafka]}}
               :defaults {:source :redis}}]
      (is (= :jetstream (routing/source-for-topic cfg :orders)))))
  (testing "falls back to default when no topic source configured"
    (let [cfg {:topics {:orders {}}
               :defaults {:source :in-memory}}]
      (is (= :in-memory (routing/source-for-topic cfg :orders))))))

(deftest sources-for-topic-resolution
  (testing "prefers explicit :sources vector"
    (let [cfg {:topics {:orders {:sources [:redis :kafka]}}
               :defaults {:source :in-memory}}]
      (is (= [:redis :kafka] (routing/sources-for-topic cfg :orders)))))
  (testing "accepts sequential :sources values"
    (let [cfg {:topics {:orders {:sources '(:redis :kafka)}}
               :defaults {:source :in-memory}}]
      (is (= [:redis :kafka] (routing/sources-for-topic cfg :orders)))))
  (testing "wraps single :source into a vector"
    (let [cfg {:topics {:orders {:source :kafka}}
               :defaults {:source :in-memory}}]
      (is (= [:kafka] (routing/sources-for-topic cfg :orders)))))
  (testing "wraps default source into a vector when unset"
    (let [cfg {:topics {:orders {}}
               :defaults {:source :redis}}]
      (is (= [:redis] (routing/sources-for-topic cfg :orders))))))
