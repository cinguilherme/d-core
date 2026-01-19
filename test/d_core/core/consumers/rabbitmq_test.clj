(ns d-core.core.consumers.rabbitmq-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.helpers.codec :as h-codec]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.consumers.rabbitmq :as rabbitmq]
            [d-core.core.messaging.dead-letter :as dl]
            [d-core.core.schema :as schema])
  (:import (com.rabbitmq.client Envelope)))

(defn- make-test-ctx
  [& {:keys [codec handler subscription-schema logger dead-letter]
      :or {codec (h-codec/make-test-codec (fn [_] {:msg "test"}))
           handler identity
           subscription-schema nil
           logger (:logger (h-logger/make-test-logger))
           dead-letter nil}}]
  {:subscription-id :test-sub
   :routing {:topics {:test {}}
             :defaults {}}
   :codec codec
   :handler handler
   :dead-letter dead-letter
   :logger logger
   :topic :test
   :exchange "core"
   :queue "core.test"
   :routing-key "core.test"
   :channel :fake-channel
   :dl-cfg {:sink :test}
   :subscription-schema subscription-schema})

(defn- make-envelope
  [delivery-tag redelivered? exchange routing-key]
  (Envelope. (long delivery-tag) (boolean redelivered?) exchange routing-key))

(deftest process-delivery-happy-path
  (testing "Successful decode, validation, and handler execution ACKs the delivery"
    (let [handler-calls (atom [])
          ack-calls (atom [])
          test-logger (h-logger/make-test-logger)
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [envelope]
                          (swap! handler-calls conj envelope)
                          :ok)
               :logger (:logger test-logger))
          envelope (make-envelope 1 false "core" "core.test")
          payload (.getBytes "test-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [channel delivery-tag]
                                                         (swap! ack-calls conj {:channel channel
                                                                                :delivery-tag delivery-tag})
                                                         :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 1 (count @handler-calls)))
        (is (= {:msg {:data "test"}} (first @handler-calls)))
        (is (= 1 (count @ack-calls)))
        (is (= {:channel :fake-channel :delivery-tag 1} (first @ack-calls)))))))

(deftest process-delivery-codec-decode-failure
  (testing "Codec decode failure triggers poison handling and ACKs"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ack-calls (atom [])
          dl-calls (atom [])
          handler-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec
                       (fn [_] (throw (ex-info "Bad codec" {:type :codec-error}))))
               :handler (fn [envelope]
                          (swap! handler-calls conj envelope)
                          :ok)
               :logger (:logger test-logger)
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope
                                                      :error-info error-info
                                                      :opts opts})
                                {:ok true})))
          envelope (make-envelope 2 false "core" "core.test")
          payload (.getBytes "bad-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [_channel delivery-tag]
                                                         (swap! ack-calls conj delivery-tag)
                                                         :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 0 (count @handler-calls)))
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (= :codec-decode-failed (get-in dl-call [:error-info :failure/type])))
          (is (= false (get-in dl-call [:error-info :retriable?])))
          (is (= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.rabbitmq/rabbitmq-poison-message (:event %)) @logs))))))

(deftest process-delivery-schema-validation-failure
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
          envelope (make-envelope 3 false "core" "core.test")
          payload (.getBytes "test-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [_channel delivery-tag]
                                                         (swap! ack-calls conj delivery-tag)
                                                         :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 0 (count @handler-calls)))
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (= :schema-invalid (get-in dl-call [:error-info :failure/type])))
          (is (= false (get-in dl-call [:error-info :retriable?])))
          (is (= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.rabbitmq/rabbitmq-poison-message (:event %)) @logs))))))

(deftest process-delivery-handler-failure-with-dlq
  (testing "Handler failure with DLQ configured sends to dead-letter and ACKs"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ack-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [_envelope]
                          (throw (ex-info "Handler failed" {:type :handler-error})))
               :logger (:logger test-logger)
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope
                                                      :error-info error-info
                                                      :opts opts})
                                {:ok true})))
          envelope (make-envelope 4 false "core" "core.test")
          payload (.getBytes "test-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [_channel delivery-tag]
                                                         (swap! ack-calls conj delivery-tag)
                                                         :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (instance? Exception (get-in dl-call [:error-info :error])))
          (is (string? (get-in dl-call [:error-info :stacktrace])))
          (is (not= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.rabbitmq/rabbitmq-handler-failed (:event %)) @logs))
        (is (some #(= :d-core.core.consumers.rabbitmq/rabbitmq-dead-letter-success (:event %)) @logs))))))

(deftest process-delivery-handler-failure-dlq-send-fails
  (testing "Handler failure where DLQ send fails does NOT ACK"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ack-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [_envelope]
                          (throw (ex-info "Handler failed" {:type :handler-error})))
               :logger (:logger test-logger)
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope
                                                      :error-info error-info
                                                      :opts opts})
                                {:ok false :error "DLQ write failed"})))
          envelope (make-envelope 5 false "core" "core.test")
          payload (.getBytes "test-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [_channel delivery-tag]
                                                         (swap! ack-calls conj delivery-tag)
                                                         :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 1 (count @dl-calls)))
        (is (= 0 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.rabbitmq/rabbitmq-dead-letter-failed (:event %)) @logs))))))

(deftest process-delivery-handler-failure-no-dlq
  (testing "Handler failure without DLQ configured logs warning and does NOT ACK"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ack-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [_envelope]
                          (throw (ex-info "Handler failed" {:type :handler-error})))
               :logger (:logger test-logger)
               :dead-letter nil)
          envelope (make-envelope 6 false "core" "core.test")
          payload (.getBytes "test-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [_channel delivery-tag]
                                                         (swap! ack-calls conj delivery-tag)
                                                         :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 0 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.rabbitmq/no-dlq-configured (:event %)) @logs))))))

(deftest process-delivery-poison-without-dlq
  (testing "Poison messages without DLQ still ACK to skip them"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ack-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec
                       (fn [_] (throw (ex-info "Bad codec" {:type :codec-error}))))
               :handler (fn [_] :ok)
               :logger (:logger test-logger)
               :dead-letter nil)
          envelope (make-envelope 7 true "core" "core.test")
          payload (.getBytes "bad-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [_channel delivery-tag]
                                                         (swap! ack-calls conj delivery-tag)
                                                         :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.rabbitmq/rabbitmq-poison-message (:event %)) @logs))
        (is (some #(= :d-core.core.consumers.rabbitmq/no-dlq-configured (:event %)) @logs))))))

(deftest process-delivery-envelope-enrichment
  (testing "Dead-letter envelopes include RabbitMQ-specific metadata"
    (let [test-logger (h-logger/make-test-logger)
          dl-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec
                       (fn [_] (throw (ex-info "Bad codec" {:type :codec-error}))))
               :handler (fn [_] :ok)
               :logger (:logger test-logger)
               :dead-letter (reify dl/DeadLetterProtocol
                              (send-dead-letter! [_ envelope error-info opts]
                                (swap! dl-calls conj {:envelope envelope
                                                      :error-info error-info
                                                      :opts opts})
                                {:ok true})))
          envelope (make-envelope 8 false "core" "core.test")
          payload (.getBytes "test-payload" "UTF-8")]
      (with-redefs [d-core.core.consumers.rabbitmq/ack! (fn [_channel _delivery-tag] :ok)]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (= 1 (count @dl-calls)))
        (let [envelope (get-in @dl-calls [0 :envelope])]
          (is (some? (get-in envelope [:metadata :dlq])))
          (is (= :test (get-in envelope [:metadata :dlq :topic])))
          (is (= :test-sub (get-in envelope [:metadata :dlq :subscription-id])))
          (is (= :rabbitmq (get-in envelope [:metadata :dlq :runtime])))
          (is (= "core" (get-in envelope [:metadata :dlq :source :exchange])))
          (is (= "core.test" (get-in envelope [:metadata :dlq :source :queue])))
          (is (= "core.test" (get-in envelope [:metadata :dlq :source :routing-key])))
          (is (= 8 (get-in envelope [:metadata :dlq :source :delivery-tag])))
          (is (= false (get-in envelope [:metadata :dlq :source :redelivered])))
          (is (= {:format :bytes-base64 :data "dGVzdC1wYXlsb2Fk"}
                 (get-in envelope [:metadata :dlq :raw-payload]))))))))

(deftest process-delivery-unexpected-exception
  (testing "Unexpected exceptions in process-delivery! are logged but don't crash"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [_envelope] :ok)
               :subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                                     :strictness :strict}
               :logger (:logger test-logger)
               :dead-letter nil)
          envelope (make-envelope 9 false "core" "core.test")
          payload (.getBytes "test-payload" "UTF-8")]
      (with-redefs [schema/validate! (fn [& _]
                                       (throw (ex-info "Unexpected error"
                                                       {:failure/type :unknown})))]
        (#'rabbitmq/process-delivery! ctx envelope payload)
        (is (some #(= :d-core.core.consumers.rabbitmq/rabbitmq-loop-failed (:event %)) @logs))))))
