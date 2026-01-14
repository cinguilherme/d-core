(ns d-core.core.consumers.in-mem-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.helpers.logger :as h-logger]
            [d-core.queue :as q]
            [d-core.core.consumers.consumer :as consumer]
            [d-core.core.messaging.dead-letter :as dl]))

(defn- wait-until
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (pred) true
        (< (System/currentTimeMillis) deadline)
        (do
          (Thread/sleep 10)
          (recur))
        :else false))))

(deftest in-memory-schema-validation-failure
  (testing "Schema validation failure sends to dead-letter with :poison status"
    (let [queue (atom clojure.lang.PersistentQueue/EMPTY)
          handler-calls (atom [])
          dl-calls (atom [])
          test-logger (h-logger/make-test-logger)
          stop? (atom false)
          subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                               :strictness :strict}
          routing {:topics {:test {}}
                   :defaults {}}
          thread (#'consumer/start-subscription!
                  {:subscription-id :test-sub
                   :queue queue
                   :poll-ms 5
                   :stop? stop?
                   :handler (fn [envelope]
                              (swap! handler-calls conj envelope)
                              :ok)
                   :dead-letter (reify dl/DeadLetterProtocol
                                  (send-dead-letter! [_ envelope error-info opts]
                                    (swap! dl-calls conj {:envelope envelope
                                                          :error-info error-info
                                                          :opts opts})
                                    {:ok true}))
                   :logger (:logger test-logger)
                   :routing routing
                   :topic :test
                   :subscription-schema subscription-schema})]
      (try
        (q/enqueue! queue {:msg {:data "missing-required"}})
        (is (wait-until #(= 1 (count @dl-calls)) 500))
        (is (= 0 (count @handler-calls)))
        (let [envelope (get-in @dl-calls [0 :envelope])]
          (is (= :poison (get-in envelope [:metadata :dlq :status])))
          (is (= :in-memory (get-in envelope [:metadata :dlq :runtime])))
          (is (= :test (get-in envelope [:metadata :dlq :topic])))
          (is (= :test-sub (get-in envelope [:metadata :dlq :subscription-id])))
          (is (= :test (get-in envelope [:metadata :dlq :source :topic])))
          (is (= :test-sub (get-in envelope [:metadata :dlq :source :subscription-id]))))
        (finally
          (reset! stop? true)
          (deref thread 500 nil))))))

(deftest in-memory-schema-validation-passes
  (testing "Valid messages pass schema validation and execute handler"
    (let [queue (atom clojure.lang.PersistentQueue/EMPTY)
          handler-calls (atom [])
          dl-calls (atom [])
          test-logger (h-logger/make-test-logger)
          stop? (atom false)
          subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                               :strictness :strict}
          routing {:topics {:test {}}
                   :defaults {}}
          thread (#'consumer/start-subscription!
                  {:subscription-id :test-sub
                   :queue queue
                   :poll-ms 5
                   :stop? stop?
                   :handler (fn [envelope]
                              (swap! handler-calls conj envelope)
                              :ok)
                   :dead-letter (reify dl/DeadLetterProtocol
                                  (send-dead-letter! [_ envelope error-info opts]
                                    (swap! dl-calls conj {:envelope envelope
                                                          :error-info error-info
                                                          :opts opts})
                                    {:ok true}))
                   :logger (:logger test-logger)
                   :routing routing
                   :topic :test
                   :subscription-schema subscription-schema})]
      (try
        (q/enqueue! queue {:msg {:required-field 42}})
        (is (wait-until #(= 1 (count @handler-calls)) 500))
        (is (= {:msg {:required-field 42}} (first @handler-calls)))
        (is (= 0 (count @dl-calls)))
        (finally
          (reset! stop? true)
          (deref thread 500 nil))))))
