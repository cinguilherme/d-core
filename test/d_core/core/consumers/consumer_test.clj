(ns d-core.core.consumers.consumer-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [d-core.queue :as q]
            [d-core.helpers.logger :as h-logger]
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
            (is (= {:sink :producer :max-retries 3}
                   (get-in envelope [:metadata :dlq :deadletter])))))
        (finally
          (ig/halt-key! :d-core.core.consumers.consumer/consumer component))))))
