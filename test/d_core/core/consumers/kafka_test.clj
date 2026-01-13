(ns d-core.core.consumers.kafka-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.consumers.kafka :as kafka]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.clients.kafka.client :as kc]
            [d-core.core.messaging.dead-letter :as dl]
            [duct.logger :as logger]))

;; Test fixtures and helpers

(defn- make-test-codec
  "Returns a codec that can be configured to succeed or fail decode."
  [decode-fn]
  (reify codec/Codec
    (decode [_ payload]
      (decode-fn payload))
    (encode [_ value]
      (.getBytes (pr-str value) "UTF-8"))))

(defn- make-test-logger
  "Returns a logger that records all log calls and the atom of logs."
  []
  (let [logs (atom [])]
    {:logger (reify logger/Logger
               (-log [_ level ns-str file line id event data]
                 (swap! logs conj {:level level :event event :data data})))
     :logs logs}))

(defn- make-test-record
  "Creates a minimal Kafka record map for testing."
  [value-bytes]
  {:topic "core.test"
   :partition 0
   :offset 123
   :timestamp 1234567890
   :key nil
   :value value-bytes
   :headers {}})

(defn- make-test-ctx
  "Creates a minimal context map for process-record! testing."
  [& {:keys [codec handler subscription-schema logger dead-letter routing]
      :or {codec (make-test-codec (fn [_] {:msg "test"}))
           handler identity
           subscription-schema nil
           logger (:logger (make-test-logger))  ; Default no-op logger
           dead-letter nil
           routing {:topics {:test {}}
                    :defaults {}}}}]
  {:subscription-id :test-sub
   :kafka {:bootstrap-servers "localhost:29092"}
   :routing routing
   :codec codec
   :handler handler
   :dead-letter dead-letter
   :logger logger
   :topic :test
   :kafka-topic "core.test"
   :group-id "test-group"
   :subscription-schema subscription-schema})

;; Tests for process-record!

(deftest process-record-happy-path
  (testing "Successful decode, validation, and handler execution commits offset"
    (let [handler-calls (atom [])
          commit-calls (atom [])
          test-logger (make-test-logger)
          ctx (make-test-ctx
                :codec (make-test-codec (fn [_] {:msg {:data "test"}}))
                :handler (fn [envelope]
                           (swap! handler-calls conj envelope)
                           :ok)
                :logger (:logger test-logger))
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        (is (= 1 (count @handler-calls)))
        (is (= {:msg {:data "test"}} (first @handler-calls)))
        (is (= 1 (count @commit-calls)))
        (is (= :fake-consumer (first @commit-calls)))))))

(deftest process-record-codec-decode-failure
  (testing "Codec decode failure triggers poison handling with :codec-decode-failed"
    (let [test-logger (make-test-logger)
          logs (:logs test-logger)
          commit-calls (atom [])
          dl-calls (atom [])
          handler-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec
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
          consumer :fake-consumer
          record (make-test-record (.getBytes "bad-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        ;; Handler should NOT be called
        (is (= 0 (count @handler-calls)))
        ;; Poison message should trigger dead-letter send
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (= :codec-decode-failed (get-in dl-call [:error-info :failure/type])))
          (is (= false (get-in dl-call [:error-info :retriable?])))
          (is (= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        ;; Poison messages are committed to skip them
        (is (= 1 (count @commit-calls)))
        ;; Logger should record the poison event
        (is (some #(= :d-core.core.consumers.kafka/kafka-poison-message (:event %)) @logs))))))

(deftest process-record-schema-validation-failure
  (testing "Schema validation failure triggers poison handling with :schema-invalid"
    (let [test-logger (make-test-logger)
          logs (:logs test-logger)
          commit-calls (atom [])
          dl-calls (atom [])
          handler-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec (fn [_] {:msg {:data "test"}}))
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
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        ;; Handler should NOT be called
        (is (= 0 (count @handler-calls)))
        ;; Schema validation failure should trigger dead-letter send
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (= :schema-invalid (get-in dl-call [:error-info :failure/type])))
          (is (= false (get-in dl-call [:error-info :retriable?])))
          (is (= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        ;; Poison messages are committed to skip them
        (is (= 1 (count @commit-calls)))
        ;; Logger should record the poison event
        (is (some #(= :d-core.core.consumers.kafka/kafka-poison-message (:event %)) @logs))))))

(deftest process-record-handler-failure-with-dlq
  (testing "Handler failure with DLQ configured sends to dead-letter and commits"
    (let [test-logger (make-test-logger)
          logs (:logs test-logger)
          commit-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec (fn [_] {:msg {:data "test"}}))
                :handler (fn [_envelope]
                           (throw (ex-info "Handler failed" {:type :handler-error})))
                :logger (:logger test-logger)
                :dead-letter (reify dl/DeadLetterProtocol
                               (send-dead-letter! [_ envelope error-info opts]
                                 (swap! dl-calls conj {:envelope envelope
                                                       :error-info error-info
                                                       :opts opts})
                                 {:ok true})))
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        ;; Dead-letter should be called with handler error
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (instance? Exception (get-in dl-call [:error-info :error])))
          (is (string? (get-in dl-call [:error-info :stacktrace])))
          ;; Handler failures are NOT marked as poison (eligible for retry)
          (is (not= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        ;; Should commit after successful DLQ send
        (is (= 1 (count @commit-calls)))
        ;; Logger should record handler failure and DLQ success
        (is (some #(= :d-core.core.consumers.kafka/kafka-handler-failed (:event %)) @logs))
        (is (some #(= :d-core.core.consumers.kafka/kafka-dead-letter-success (:event %)) @logs))))))

(deftest process-record-handler-failure-dlq-send-fails
  (testing "Handler failure where DLQ send fails does NOT commit"
    (let [test-logger (make-test-logger)
          logs (:logs test-logger)
          commit-calls (atom [])
          dl-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec (fn [_] {:msg {:data "test"}}))
                :handler (fn [_envelope]
                           (throw (ex-info "Handler failed" {:type :handler-error})))
                :logger (:logger test-logger)
                :dead-letter (reify dl/DeadLetterProtocol
                               (send-dead-letter! [_ envelope error-info opts]
                                 (swap! dl-calls conj {:envelope envelope
                                                       :error-info error-info
                                                       :opts opts})
                                 {:ok false :error "DLQ write failed"})))
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        ;; Dead-letter should be attempted
        (is (= 1 (count @dl-calls)))
        ;; Should NOT commit when DLQ send fails
        (is (= 0 (count @commit-calls)))
        ;; Logger should record DLQ failure
        (is (some #(= :d-core.core.consumers.kafka/kafka-dead-letter-failed (:event %)) @logs))))))

(deftest process-record-handler-failure-no-dlq
  (testing "Handler failure without DLQ configured logs warning and does NOT commit"
    (let [test-logger (make-test-logger)
          logs (:logs test-logger)
          commit-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec (fn [_] {:msg {:data "test"}}))
                :handler (fn [_envelope]
                           (throw (ex-info "Handler failed" {:type :handler-error})))
                :logger (:logger test-logger)
                :dead-letter nil)  ; No DLQ configured
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        ;; Should NOT commit when no DLQ is available
        (is (= 0 (count @commit-calls)))
        ;; Logger should record that no DLQ is configured
        (is (some #(= :d-core.core.consumers.kafka/no-dlq-configured (:event %)) @logs))))))

(deftest process-record-poison-without-dlq
  (testing "Poison messages without DLQ still commit offset to skip them"
    (let [test-logger (make-test-logger)
          logs (:logs test-logger)
          commit-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec
                         (fn [_] (throw (ex-info "Bad codec" {:type :codec-error}))))
                :handler (fn [_] :ok)
                :logger (:logger test-logger)
                :dead-letter nil)  ; No DLQ configured
          consumer :fake-consumer
          record (make-test-record (.getBytes "bad-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        ;; Poison messages should ALWAYS commit (to skip them)
        (is (= 1 (count @commit-calls)))
        ;; Logger should record the poison event
        (is (some #(= :d-core.core.consumers.kafka/kafka-poison-message (:event %)) @logs))))))

(deftest process-record-envelope-enrichment
  (testing "Dead-letter envelopes include Kafka-specific metadata"
    (let [test-logger (make-test-logger)
          dl-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec
                         (fn [_] (throw (ex-info "Bad codec" {:type :codec-error}))))
                :handler (fn [_] :ok)
                :logger (:logger test-logger)
                :dead-letter (reify dl/DeadLetterProtocol
                               (send-dead-letter! [_ envelope error-info opts]
                                 (swap! dl-calls conj {:envelope envelope
                                                       :error-info error-info
                                                       :opts opts})
                                 {:ok true})))
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [_] {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        (is (= 1 (count @dl-calls)))
        (let [envelope (get-in @dl-calls [0 :envelope])]
          ;; Check DLQ metadata is present
          (is (some? (get-in envelope [:metadata :dlq])))
          (is (= :test (get-in envelope [:metadata :dlq :topic])))
          (is (= :test-sub (get-in envelope [:metadata :dlq :subscription-id])))
          (is (= :kafka (get-in envelope [:metadata :dlq :runtime])))
          ;; Check Kafka-specific source metadata
          (is (= "core.test" (get-in envelope [:metadata :dlq :source :kafka-topic])))
          (is (= "test-group" (get-in envelope [:metadata :dlq :source :group-id])))
          (is (= 0 (get-in envelope [:metadata :dlq :source :partition])))
          (is (= 123 (get-in envelope [:metadata :dlq :source :offset])))
          (is (= 1234567890 (get-in envelope [:metadata :dlq :source :timestamp])))
          ;; Check raw payload is preserved
          (is (some? (get-in envelope [:metadata :dlq :raw-payload]))))))))

(deftest process-record-unexpected-exception
  (testing "Unexpected exceptions in process-record! are logged but don't crash the loop"
    (let [test-logger (make-test-logger)
          logs (:logs test-logger)
          ctx (make-test-ctx
                :codec (make-test-codec (fn [_] {:msg {:data "test"}}))
                :handler (fn [_envelope]
                           ;; Throw an exception that's NOT caught by the normal paths
                           (throw (IllegalStateException. "Unexpected error")))
                :logger (:logger test-logger)
                :dead-letter nil)
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
        ;; This should not throw - the outer catch should log it
        (#'kafka/process-record! ctx consumer record)
        ;; Logger should capture the unexpected failure
        (is (some #(or (= :d-core.core.consumers.kafka/kafka-loop-failed (:event %))
                       (= :d-core.core.consumers.kafka/kafka-handler-failed (:event %)))
                  @logs)))))

(deftest process-record-schema-validation-passes
  (testing "Valid message passes schema validation and executes handler"
    (let [test-logger (make-test-logger)
          handler-calls (atom [])
          commit-calls (atom [])
          ctx (make-test-ctx
                :codec (make-test-codec (fn [_] {:msg {:required-field 42}}))
                :handler (fn [envelope]
                           (swap! handler-calls conj envelope)
                           :ok)
                :logger (:logger test-logger)
                :subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                                     :strictness :strict})
          consumer :fake-consumer
          record (make-test-record (.getBytes "test-payload" "UTF-8"))]
      (with-redefs [kc/commit! (fn [c]
                                 (swap! commit-calls conj c)
                                 {:ok true})]
        (#'kafka/process-record! ctx consumer record)
        ;; Handler should be called with validated envelope
        (is (= 1 (count @handler-calls)))
        (is (= {:msg {:required-field 42}} (first @handler-calls)))
        ;; Should commit after successful processing
        (is (= 1 (count @commit-calls)))))))
