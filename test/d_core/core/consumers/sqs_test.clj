(ns d-core.core.consumers.sqs-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.helpers.codec :as h-codec]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.consumers.sqs :as sqs]
            [d-core.core.messaging.dead-letter :as dl]))

(defn- make-test-ctx
  [& {:keys [codec handler subscription-schema logger dead-letter]
      :or {codec (h-codec/make-test-codec (fn [_] {:msg "test"}))
           handler identity
           subscription-schema nil
           logger (:logger (h-logger/make-test-logger))
           dead-letter nil}}]
  {:subscription-id :test-sub
   :routing {:topics {:test {}} :defaults {}}
   :codec codec
   :handler handler
   :dead-letter dead-letter
   :logger logger
   :topic :test
   :queue "core.test"
   :queue-url "http://localhost:9324/queue/core.test"
   :sqs :fake-sqs
   :dl-cfg {:sink :test}
   :subscription-schema subscription-schema
   :client-key :default})

(deftest process-message-happy-path
  (testing "Successful decode and handler execution deletes the message"
    (let [handler-calls (atom [])
          delete-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "ok"}}))
               :handler (fn [envelope]
                          (swap! handler-calls conj envelope)
                          :ok))
          msg {:message-id "m1" :receipt-handle "r1" :body "payload"}]
      (with-redefs [d-core.core.clients.sqs.client/delete-message!
                    (fn [sqs queue-url receipt-handle]
                      (swap! delete-calls conj {:sqs sqs :queue-url queue-url :receipt-handle receipt-handle})
                      :ok)]
        (#'sqs/process-message! ctx msg)
        (is (= [{:msg {:data "ok"}}] @handler-calls))
        (is (= [{:sqs :fake-sqs :queue-url "http://localhost:9324/queue/core.test" :receipt-handle "r1"}]
               @delete-calls))))))

(deftest process-message-codec-decode-failure
  (testing "Codec decode failure triggers poison handling and delete"
    (let [test-logger (h-logger/make-test-logger)
          delete-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] (throw (ex-info "bad" {}))))
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope :error-info error-info :opts opts})
                                {:ok true}))
               :logger (:logger test-logger))
          msg {:message-id "m2" :receipt-handle "r2" :body "payload"}]
      (with-redefs [d-core.core.clients.sqs.client/delete-message!
                    (fn [_sqs _queue-url receipt-handle]
                      (swap! delete-calls conj receipt-handle)
                      :ok)]
        (#'sqs/process-message! ctx msg)
        (is (= ["r2"] @delete-calls))
        (is (= 1 (count @dl-calls)))
        (is (= :codec-decode-failed (get-in (first @dl-calls) [:error-info :failure/type])))))))

(deftest process-message-schema-invalid
  (testing "Schema invalid triggers poison handling and delete"
    (let [delete-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:a 1 :extra "x"}}))
               :subscription-schema {:schema [:map {:closed true} [:a :int]]
                                     :strictness :strict}
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope :error-info error-info :opts opts})
                                {:ok true})))
          msg {:message-id "m3" :receipt-handle "r3" :body "payload"}]
      (with-redefs [d-core.core.clients.sqs.client/delete-message!
                    (fn [_sqs _queue-url receipt-handle]
                      (swap! delete-calls conj receipt-handle)
                      :ok)]
        (#'sqs/process-message! ctx msg)
        (is (= ["r3"] @delete-calls))
        (is (= 1 (count @dl-calls)))
        (is (= :schema-invalid (get-in (first @dl-calls) [:error-info :failure/type])))))))

(deftest process-message-handler-failure-with-dlq
  (testing "Handler failure with DLQ configured sends to dead-letter and deletes"
    (let [delete-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "ok"}}))
               :handler (fn [_] (throw (ex-info "boom" {})))
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope :error-info error-info :opts opts})
                                {:ok true})))
          msg {:message-id "m4" :receipt-handle "r4" :body "payload"}]
      (with-redefs [d-core.core.clients.sqs.client/delete-message!
                    (fn [_sqs _queue-url receipt-handle]
                      (swap! delete-calls conj receipt-handle)
                      :ok)]
        (#'sqs/process-message! ctx msg)
        (is (= ["r4"] @delete-calls))
        (is (= 1 (count @dl-calls)))))))

(deftest process-message-handler-failure-no-dlq
  (testing "Handler failure without DLQ does not delete"
    (let [delete-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "ok"}}))
               :handler (fn [_] (throw (ex-info "boom" {})))
               :dead-letter nil)
          msg {:message-id "m5" :receipt-handle "r5" :body "payload"}]
      (with-redefs [d-core.core.clients.sqs.client/delete-message!
                    (fn [_sqs _queue-url receipt-handle]
                      (swap! delete-calls conj receipt-handle)
                      :ok)]
        (#'sqs/process-message! ctx msg)
        (is (empty? @delete-calls))))))
