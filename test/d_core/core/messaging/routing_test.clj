(ns d-core.core.messaging.routing-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [d-core.core.messaging.routing :as routing]
            [d-core.tracing :as tracing]))

(def default-routing
  {:defaults {:source :redis
              :deadletter {:sink :hybrid
                           :policy :default
                           :max-attempts 3
                           :delay-ms 100
                           :suffix ".dl"}}
   :topics {:default {:source :redis
                      :stream "core:default"
                      :group "core"}
            :sample-fail {:source :redis
                          :stream "sample:fail-sample"
                          :group "sample"}
            :test-queue {:source :in-memory}
            :kafka-test {:source :kafka
                         :kafka-topic "core.kafka_test"
                         :group "core"}
            :jetstream-test {:source :jetstream
                             :subject "core.jetstream_test"
                             :stream "core_jetstream_test"
                             :durable "core_jetstream_test"}}
   :subscriptions {:default {:source :redis
                             :topic :default
                             :handler :log-consumed
                             :options {:block-ms 5000}}
                   :sample-fail {:source :redis
                                 :topic :sample-fail
                                 :handler :log-consumed
                                 :options {:block-ms 5000}
                                 :deadletter {:sink :hybrid
                                              :policy :default
                                              :max-attempts 3
                                              :delay-ms 100
                                              :suffix ".dl"}}
                   :kafka-test {:source :kafka
                                :topic :kafka-test
                                :handler :log-consumed
                                :options {:poll-ms 250}}
                   :jetstream-test {:source :jetstream
                                    :topic :jetstream-test
                                    :handler :log-consumed
                                    :options {:pull-batch 1
                                              :expires-ms 1000}}}})

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

(deftest init-key-merges-default-routing-and-resolves-handlers
  (testing "default-routing + overrides deep-merge and resolve handlers"
    (let [calls (atom [])
          fail-once? (atom false)
          handlers {:log-consumed (fn [envelope]
                                    (swap! calls conj {:handler :log-consumed
                                                       :ctx tracing/*ctx*
                                                       :envelope envelope}))
                    :fail-once (fn [envelope]
                                 (if (compare-and-set! fail-once? false true)
                                   (throw (ex-info "fail-once" {:failure/type :transient}))
                                   (swap! calls conj {:handler :fail-once
                                                      :ctx tracing/*ctx*
                                                      :envelope envelope})))}
          overrides {:handlers handlers
                     :subscriptions {:sample-fail {:source :in-memory
                                                   :topic :test-queue
                                                   :handler :fail-once}}}
          merged (ig/init-key :d-core.core.messaging/routing
                              {:default-routing default-routing
                               :overrides overrides})]
      (is (fn? (get-in merged [:subscriptions :default :handler])))
      (is (fn? (get-in merged [:subscriptions :sample-fail :handler])))
      (is (= :in-memory (get-in merged [:subscriptions :sample-fail :source])))
      (is (= ".dl" (get-in merged [:subscriptions :sample-fail :deadletter :suffix])))
      (is (contains? (:handlers merged) :log-consumed))
      (is (contains? (:handlers merged) :fail-once))
      (let [parent {:trace-id "t1" :span-id "s1" :parent-span-id nil}
            envelope {:msg {:id 1}
                      :metadata {:trace (tracing/encode-ctx parent)}}
            handler (get-in merged [:subscriptions :default :handler])]
        (handler envelope)
        (is (= 1 (count @calls)))
        (let [{:keys [ctx]} (first @calls)]
          (is (= "t1" (:trace-id ctx)))
          (is (= "s1" (:parent-span-id ctx))))))))

(deftest init-key-resolves-handlers-with-plain-routing
  (testing "plain routing map resolves keyword handlers"
    (let [calls (atom [])
          cfg (ig/init-key :d-core.core.messaging/routing
                           {:handlers {:log-consumed (fn [_] (swap! calls conj :ok))}
                            :subscriptions {:sub {:handler :log-consumed}}})]
      ((get-in cfg [:subscriptions :sub :handler]) {:msg {:ok true}})
      (is (= [:ok] @calls)))))
