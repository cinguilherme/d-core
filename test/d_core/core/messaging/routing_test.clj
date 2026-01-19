(ns d-core.core.messaging.routing-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.messaging.routing :as routing]))

(deftest publish-targets-resolution
  (testing "returns empty vector when no targets configured"
    (let [cfg {:publish {}}]
      (is (= [] (routing/publish-targets cfg :orders)))))
  (testing "wraps a single target map into a vector"
    (let [cfg {:publish {:orders {:targets {:producer :kafka-primary
                                            :kafka-topic "core.orders"}}}}]
      (is (= [{:producer :kafka-primary :kafka-topic "core.orders"}]
             (routing/publish-targets cfg :orders)))))
  (testing "preserves explicit target order"
    (let [cfg {:publish {:orders {:targets [{:producer :redis-primary :stream "core:orders"}
                                            {:producer :kafka-dr :kafka-topic "core.orders"}]}}}]
      (is (= [:redis-primary :kafka-dr]
             (mapv :producer (routing/publish-targets cfg :orders)))))))

(deftest deadletter-config-merges-subscription-overrides
  (testing "subscription overrides win over topic and defaults"
    (let [cfg {:defaults {:deadletter {:sink :storage :suffix ".dl"}}
               :topics {:orders {:deadletter {:sink :producer :max-attempts 3}}}
               :subscriptions {:orders-worker {:topic :orders
                                               :deadletter {:max-attempts 7
                                                            :delay-ms 1000}}}}]
      (is (= {:sink :producer :suffix ".dl" :max-attempts 7 :delay-ms 1000}
             (routing/deadletter-config cfg :orders :orders-worker))))))
