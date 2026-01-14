(ns d-core.core.consumers.jetstream-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.helpers.codec :as h-codec]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.consumers.jetstream :as jetstream]
            [d-core.core.messaging.dead-letter :as dl])
  (:import (io.nats.client Message)))

(defn- make-test-message
  [payload ack-calls]
  (proxy [Message] []
    (getData [] payload)
    (ack [] (swap! ack-calls conj :ack))))

(defn- make-test-ctx
  [& {:keys [codec handler subscription-schema logger dead-letter routing]
      :or {codec (h-codec/make-test-codec (fn [_] {:msg "test"}))
           handler identity
           subscription-schema nil
           logger (:logger (h-logger/make-test-logger))
           dead-letter nil
           routing {:topics {:test {}}
                    :defaults {}}}}]
  {:subscription-id :test-sub
   :routing routing
   :codec codec
   :handler handler
   :dead-letter dead-letter
   :logger logger
   :topic :test
   :subject "core.test"
   :stream "core_test"
   :durable "core_test"
   :subscription-schema subscription-schema})

(deftest process-message-schema-validation-failure
  (testing "Schema validation failure triggers poison handling and ACKs"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ack-calls (atom [])
          dl-calls (atom [])
          handler-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [envelope]
                          (swap! handler-calls conj envelope)
                          :ok)
               :subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                                     :strictness :strict}
               :logger (:logger test-logger)
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope
                                                      :error-info error-info
                                                      :opts opts})
                                {:ok true})))
          msg (make-test-message (.getBytes "test-payload" "UTF-8") ack-calls)]
      (#'jetstream/process-message! ctx msg)
      (is (= 0 (count @handler-calls)))
      (is (= 1 (count @dl-calls)))
      (let [dl-call (first @dl-calls)]
        (is (= :schema-invalid (get-in dl-call [:error-info :failure/type])))
        (is (= false (get-in dl-call [:error-info :retriable?])))
        (is (= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
      (is (= 1 (count @ack-calls)))
      (is (some #(= :d-core.core.consumers.jetstream/jetstream-poison-message (:event %))
                @logs)))))

(deftest process-message-schema-validation-passes
  (testing "Valid message passes schema validation and executes handler"
    (let [test-logger (h-logger/make-test-logger)
          handler-calls (atom [])
          ack-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:required-field 42}}))
               :handler (fn [envelope]
                          (swap! handler-calls conj envelope)
                          :ok)
               :logger (:logger test-logger)
               :subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                                     :strictness :strict}
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope
                                                      :error-info error-info
                                                      :opts opts})
                                {:ok true})))
          msg (make-test-message (.getBytes "test-payload" "UTF-8") ack-calls)]
      (#'jetstream/process-message! ctx msg)
      (is (= 1 (count @handler-calls)))
      (is (= {:msg {:required-field 42}} (first @handler-calls)))
      (is (= 1 (count @ack-calls)))
      (is (= 0 (count @dl-calls))))))
