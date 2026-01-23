(ns d-core.core.consumers.consumer-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [d-core.queue :as q]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.messaging.routing]
            [d-core.core.consumers.consumer]
            [d-core.core.messaging.dead-letter.protocol :as dlp]))

(defn- make-queues
  []
  {:topics->queue (atom {})})

(defn- wait-for
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 10) (recur))
          false)))))

(defn- recording-dead-letter
  [calls]
  (reify dlp/DeadLetterProtocol
    (send-dead-letter! [_ envelope error-info opts]
      (swap! calls conj {:envelope envelope :error-info error-info :opts opts})
      {:ok true :sink :test})))

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

(defonce ^:private handler-calls (atom []))
(defonce ^:private fail-once-state (atom false))

(defn- reset-handler-state!
  []
  (reset! handler-calls [])
  (reset! fail-once-state false))

(defn log-consumed
  [envelope]
  (swap! handler-calls conj {:handler :log-consumed :envelope envelope}))

(defn fail-once
  [envelope]
  (if (compare-and-set! fail-once-state false true)
    (throw (ex-info "fail-once" {:failure/type :transient}))
    (swap! handler-calls conj {:handler :fail-once :envelope envelope})))

(deftest init-filters-in-memory-subscriptions
  (testing "only in-memory subscriptions start threads"
    (let [queues (make-queues)
          routing {:subscriptions {:in-mem {:topic :orders :handler identity :source :in-memory}
                                   :kafka {:topic :orders :handler identity :source :kafka}}}
          logger (:logger (h-logger/make-test-logger))
          component (ig/init-key :d-core.core.consumers.consumer/consumer
                                 {:queues queues
                                  :routing routing
                                  :default-poll-ms 5
                                  :logger logger})]
      (try
        (is (= #{:in-mem} (set (keys (:threads component)))))
        (finally
          (ig/halt-key! :d-core.core.consumers.consumer/consumer component))))))


(deftest publish-config-does-not-start-consumers
  (testing "publish targets do not start consumers without subscriptions"
    (let [queues (make-queues)
          routing {:topics {:orders {}}
                   :publish {:orders {:targets [{:producer :kafka-primary
                                                 :kafka-topic "core.orders"}]}}}
          logger (:logger (h-logger/make-test-logger))
          component (ig/init-key :d-core.core.consumers.consumer/consumer
                                 {:queues queues
                                  :routing routing
                                  :default-poll-ms 5
                                  :logger logger})]
      (try
        (is (empty? (:threads component)))
        (finally
          (ig/halt-key! :d-core.core.consumers.consumer/consumer component))))))

(deftest in-memory-consumer-handles-default-topic
  (testing "subscription with no topic consumes from :default"
    (let [queues (make-queues)
          handled (atom [])
          handler (fn [item] (swap! handled conj item))
          routing {:subscriptions {:sub {:handler handler :source :in-memory}}
                   :topics {}}
          logger (:logger (h-logger/make-test-logger))
          component (ig/init-key :d-core.core.consumers.consumer/consumer
                                 {:queues queues
                                  :routing routing
                                  :default-poll-ms 5
                                  :logger logger})]
      (try
        (let [queue (q/get-queue! queues :default)
              payload {:msg {:a 1}}]
          (q/enqueue! queue payload)
          (is (wait-for #(= 1 (count @handled)) 500))
          (is (= payload (first @handled))))
        (finally
          (ig/halt-key! :d-core.core.consumers.consumer/consumer component))))))

(deftest in-memory-consumer-sends-dead-letter-on-schema-invalid
  (testing "schema violations trigger dead-letter with poison status"
    (let [queues (make-queues)
          handled (atom [])
          handler (fn [item] (swap! handled conj item))
          dlq-calls (atom [])
          dead-letter (recording-dead-letter dlq-calls)
          routing {:defaults {:deadletter {:sink :producer}}
                   :topics {:orders {:deadletter {:max-retries 3}}}
                   :subscriptions {:sub {:topic :orders
                                         :handler handler
                                         :source :in-memory
                                         :deadletter {:max-retries 7
                                                      :delay-ms 1000}
                                         :schema {:schema [:map {:closed true} [:a :int]]
                                                  :strictness :strict}}}}
          logger (:logger (h-logger/make-test-logger))
          component (ig/init-key :d-core.core.consumers.consumer/consumer
                                 {:queues queues
                                  :routing routing
                                  :default-poll-ms 5
                                  :dead-letter dead-letter
                                  :logger logger})]
      (try
        (let [queue (q/get-queue! queues :orders)
              payload {:msg {:a 1 :extra "x"}}]
          (q/enqueue! queue payload)
          (is (wait-for #(= 1 (count @dlq-calls)) 500))
          (is (= [] @handled))
          (let [{:keys [envelope]} (first @dlq-calls)]
            (is (= :orders (get-in envelope [:metadata :dlq :topic])))
            (is (= :poison (get-in envelope [:metadata :dlq :status])))
            (is (= {:sink :producer :max-retries 7 :delay-ms 1000}
                   (get-in envelope [:metadata :dlq :deadletter])))
            (is (= :in-memory (get-in envelope [:metadata :dlq :producer])))))
        (finally
          (ig/halt-key! :d-core.core.consumers.consumer/consumer component))))))

(deftest routing-defaults-overrides-in-memory-replay
  (testing "default-routing + overrides resolve handlers and allow DLQ replay"
    (reset-handler-state!)
    (let [queues (make-queues)
          dlq-calls (atom [])
          dead-letter (recording-dead-letter dlq-calls)
          overrides {:handlers {:log-consumed log-consumed
                                :fail-once fail-once}
                     :topics {:test-queue-log {:source :in-memory}}
                     :subscriptions {:default {:source :in-memory
                                               :topic :test-queue-log
                                               :handler :log-consumed}
                                     :sample-fail {:source :in-memory
                                                   :topic :test-queue
                                                   :handler :fail-once
                                                   :options {:block-ms 10}}}}
          routing (ig/init-key :d-core.core.messaging/routing
                               {:default-routing default-routing
                                :overrides overrides})
          logger (:logger (h-logger/make-test-logger))
          component (ig/init-key :d-core.core.consumers.consumer/consumer
                                 {:queues queues
                                  :routing routing
                                  :default-poll-ms 5
                                  :dead-letter dead-letter
                                  :logger logger})]
      (try
        (let [log-queue (q/get-queue! queues :test-queue-log)
              fail-queue (q/get-queue! queues :test-queue)
              payload {:msg {:id 1}}]
          (q/enqueue! log-queue payload)
          (is (wait-for #(some (fn [call] (= :log-consumed (:handler call)))
                               @handler-calls)
                        500))
          (q/enqueue! fail-queue payload)
          (is (wait-for #(= 1 (count @dlq-calls)) 750))
          (is (not-any? (fn [call] (= :fail-once (:handler call))) @handler-calls))
          (let [{:keys [envelope]} (first @dlq-calls)]
            (is (= :eligible (get-in envelope [:metadata :dlq :status])))
            (q/enqueue! fail-queue envelope))
          (is (wait-for #(some (fn [call] (= :fail-once (:handler call)))
                               @handler-calls)
                        750)))
        (finally
          (ig/halt-key! :d-core.core.consumers.consumer/consumer component))))))
