(ns d-core.core.consumers.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.helpers.codec :as h-codec]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.consumers.redis :as redis]
            [d-core.core.messaging.dead-letter :as dl]
            [d-core.core.schema :as schema]))

(defn- make-test-ctx
  [& {:keys [codec handler subscription-schema logger dead-letter]
      :or {codec (h-codec/make-test-codec (fn [_] {:msg "test"}))
           handler identity
           subscription-schema nil
           logger (:logger (h-logger/make-test-logger))
           dead-letter nil}}]
  {:subscription-id :test-sub
   :conn :fake-conn
   :stream "core:test"
   :group "core"
   :consumer-name "test-consumer"
   :codec codec
   :handler handler
   :dead-letter dead-letter
   :logger logger
   :topic :test
   :routing {:topics {:test {}}
             :defaults {}}
   :dl-cfg {:sink :test}
   :subscription-schema subscription-schema})

(defn- make-test-fields
  [payload]
  ["payload" payload])

(deftest process-entry-happy-path
  (testing "Successful decode, validation, and handler execution ACKs the entry"
    (let [handler-calls (atom [])
          ack-calls (atom [])
          test-logger (h-logger/make-test-logger)
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [envelope]
                          (swap! handler-calls conj envelope)
                          :ok)
               :logger (:logger test-logger))
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [conn stream group r-id]
                                   (swap! ack-calls conj {:conn conn
                                                          :stream stream
                                                          :group group
                                                          :redis-id r-id})
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 1 (count @handler-calls)))
        (is (= {:msg {:data "test"}} (first @handler-calls)))
        (is (= 1 (count @ack-calls)))
        (is (= {:conn :fake-conn
                :stream "core:test"
                :group "core"
                :redis-id "1-0"}
               (first @ack-calls)))))))

(deftest process-entry-codec-decode-failure
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
          redis-id "1-0"
          fields (make-test-fields "bad-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group r-id]
                                   (swap! ack-calls conj r-id)
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 0 (count @handler-calls)))
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (= :codec-decode-failed (get-in dl-call [:error-info :failure/type])))
          (is (= false (get-in dl-call [:error-info :retriable?])))
          (is (= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.redis/redis-poison-message (:event %)) @logs))))))

(deftest process-entry-schema-validation-failure
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
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group r-id]
                                   (swap! ack-calls conj r-id)
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 0 (count @handler-calls)))
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (= :schema-invalid (get-in dl-call [:error-info :failure/type])))
          (is (= false (get-in dl-call [:error-info :retriable?])))
          (is (= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.redis/redis-poison-message (:event %)) @logs))))))

(deftest process-entry-handler-failure-with-dlq
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
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group r-id]
                                   (swap! ack-calls conj r-id)
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 1 (count @dl-calls)))
        (let [dl-call (first @dl-calls)]
          (is (instance? Exception (get-in dl-call [:error-info :error])))
          (is (string? (get-in dl-call [:error-info :stacktrace])))
          (is (not= :poison (get-in dl-call [:envelope :metadata :dlq :status]))))
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.redis/redis-handler-failed (:event %)) @logs))
        (is (some #(= :d-core.core.consumers.redis/redis-dead-letter-success (:event %)) @logs))))))

(deftest process-entry-handler-failure-dlq-send-fails
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
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group r-id]
                                   (swap! ack-calls conj r-id)
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 1 (count @dl-calls)))
        (is (= 0 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.redis/redis-dead-letter-failed (:event %)) @logs))))))

(deftest process-entry-handler-failure-no-dlq
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
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group r-id]
                                   (swap! ack-calls conj r-id)
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 0 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.redis/no-dlq-configured (:event %)) @logs))))))

(deftest process-entry-poison-without-dlq
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
          redis-id "1-0"
          fields (make-test-fields "bad-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group r-id]
                                   (swap! ack-calls conj r-id)
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 1 (count @ack-calls)))
        (is (some #(= :d-core.core.consumers.redis/redis-poison-message (:event %)) @logs))
        (is (some #(= :d-core.core.consumers.redis/no-dlq-configured (:event %)) @logs))))))

(deftest process-entry-envelope-enrichment
  (testing "Dead-letter envelopes include Redis-specific metadata"
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
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group _r-id] :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 1 (count @dl-calls)))
        (let [envelope (get-in @dl-calls [0 :envelope])]
          (is (some? (get-in envelope [:metadata :dlq])))
          (is (= :test (get-in envelope [:metadata :dlq :topic])))
          (is (= :test-sub (get-in envelope [:metadata :dlq :subscription-id])))
          (is (= :redis (get-in envelope [:metadata :dlq :runtime])))
          (is (= "core:test" (get-in envelope [:metadata :dlq :source :stream])))
          (is (= "core" (get-in envelope [:metadata :dlq :source :group])))
          (is (= "test-consumer" (get-in envelope [:metadata :dlq :source :consumer])))
          (is (= "1-0" (get-in envelope [:metadata :dlq :source :redis-id])))
          (is (= {:format :string :data "test-payload"}
                 (get-in envelope [:metadata :dlq :raw-payload]))))))))

(deftest process-entry-unexpected-exception
  (testing "Unexpected exceptions in process-entry! are logged but don't crash"
    (let [test-logger (h-logger/make-test-logger)
          logs (:logs test-logger)
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:data "test"}}))
               :handler (fn [_envelope] :ok)
               :subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                                     :strictness :strict}
               :logger (:logger test-logger)
               :dead-letter nil)
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [schema/validate! (fn [& _]
                                       (throw (ex-info "Unexpected error"
                                                       {:failure/type :unknown})))]
        (#'redis/process-entry! ctx redis-id fields)
        (is (some #(= :d-core.core.consumers.redis/redis-loop-failed (:event %)) @logs))))))

(deftest process-entry-schema-validation-passes
  (testing "Valid message passes schema validation and executes handler"
    (let [test-logger (h-logger/make-test-logger)
          handler-calls (atom [])
          ack-calls (atom [])
          ctx (make-test-ctx
               :codec (h-codec/make-test-codec (fn [_] {:msg {:required-field 42}}))
               :handler (fn [envelope]
                          (swap! handler-calls conj envelope)
                          :ok)
               :logger (:logger test-logger)
               :subscription-schema {:schema [:map {:closed true} [:required-field :int]]
                                     :strictness :strict})
          redis-id "1-0"
          fields (make-test-fields "test-payload")]
      (with-redefs [d-core.core.consumers.redis/ack! (fn [_conn _stream _group r-id]
                                   (swap! ack-calls conj r-id)
                                   :ok)]
        (#'redis/process-entry! ctx redis-id fields)
        (is (= 1 (count @handler-calls)))
        (is (= {:msg {:required-field 42}} (first @handler-calls)))
        (is (= 1 (count @ack-calls)))))))
