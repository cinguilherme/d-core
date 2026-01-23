(ns d-core.integration.kafka-multi-cluster-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [d-core.helpers.codec :as h-codec]
            [d-core.core.messaging.codec :as codec]
            [d-core.core.clients.kafka.client :as kc]
            [d-core.core.producers.kafka :as kafka-producer]
            [d-core.core.producers.common :as common-producer]
            [d-core.core.producers.protocol :as p])
  (:import (java.util UUID Properties)
           (java.util.concurrent TimeUnit)
           (org.apache.kafka.clients.admin AdminClient NewTopic)
           (org.apache.kafka.common.errors TopicExistsException)
           (java.util.concurrent TimeoutException)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))))

(defn- bootstrap
  [env-key default]
  (or (System/getenv env-key) default))

(defn- wait-for-record
  [consumer timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [records (kc/poll! consumer {:timeout-ms 250})]
        (if-let [record (first records)]
          record
          (if (< (System/currentTimeMillis) deadline)
            (recur)
            nil))))))

(defn- decode-envelope
  [codec record]
  (when record
    (codec/decode codec (:value record))))

(defn- admin-client
  [bootstrap-servers]
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" bootstrap-servers))]
    (AdminClient/create props)))

(defn- wait-for-topic
  [admin topic timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [result (try
                     (.. admin (describeTopics [topic]) all (get 2 TimeUnit/SECONDS))
                     {:ok true}
                     (catch Exception e
                       {:ok false :error e}))]
        (if (:ok result)
          true
          (if (< (System/currentTimeMillis) deadline)
            (do
              (Thread/sleep 200)
              (recur))
            (throw (:error result))))))))

(defn- ensure-topic!
  [admin topic]
  (let [new-topic (NewTopic. topic 1 (short 1))]
    (try
      (.. admin (createTopics [new-topic]) all (get 15 TimeUnit/SECONDS))
      (catch Exception e
        (let [cause (or (.getCause e) e)]
          (cond
            (instance? TopicExistsException cause) nil
            (instance? TimeoutException cause) nil
            :else (throw e)))))
    (wait-for-topic admin topic 20000)))

(deftest produce-to-multiple-kafka-clusters
  (testing "single produce fans out to two Kafka clusters"
    (if-not (integration-enabled?)
      (is true "Skipping Kafka integration test; set INTEGRATION=1")
      (let [primary-bs (bootstrap "DCORE_KAFKA_PRIMARY" "localhost:29092")
            data-bs (bootstrap "DCORE_KAFKA_DATA" "localhost:29094")
            topic-primary (str "dcore.int.orders." (UUID/randomUUID))
            topic-data (str "dcore.int.orders.data." (UUID/randomUUID))
            codec (h-codec/make-test-codec
                   (fn [payload]
                     (edn/read-string (String. ^bytes payload "UTF-8"))))
            routing {:publish {:orders {:targets [{:producer :kafka-primary
                                                   :kafka-topic topic-primary}
                                                  {:producer :kafka-data
                                                   :kafka-topic topic-data}]}}}
            client-primary (kc/make-client {:bootstrap-servers primary-bs})
            client-data (kc/make-client {:bootstrap-servers data-bs})
            producer-primary (kafka-producer/->KafkaProducer client-primary routing codec nil)
            producer-data (kafka-producer/->KafkaProducer client-data routing codec nil)
            producer (common-producer/->CommonProducer
                       :kafka-primary
                       {:kafka-primary producer-primary
                        :kafka-data producer-data}
                       routing
                       nil
                       nil)
            msg {:id (str (UUID/randomUUID))
                 :amount 12.5}
            consumer-primary (kc/make-consumer client-primary {:group-id (str "dcore-int-" (UUID/randomUUID))})
            consumer-data (kc/make-consumer client-data {:group-id (str "dcore-int-" (UUID/randomUUID))})
            admin-primary (admin-client primary-bs)
            admin-data (admin-client data-bs)]
        (try
          (ensure-topic! admin-primary topic-primary)
          (ensure-topic! admin-data topic-data)
          (kc/subscribe! consumer-primary [topic-primary])
          (kc/subscribe! consumer-data [topic-data])
          (kc/poll! consumer-primary {:timeout-ms 100})
          (kc/poll! consumer-data {:timeout-ms 100})
          (let [ack (p/produce! producer msg {:topic :orders})
                _ (is (= 2 (count ack)))
                rec-primary (wait-for-record consumer-primary 5000)
                rec-data (wait-for-record consumer-data 5000)
                env-primary (decode-envelope codec rec-primary)
                env-data (decode-envelope codec rec-data)]
            (is (some? rec-primary) "Primary Kafka cluster receives the message")
            (is (some? rec-data) "Data Kafka cluster receives the message")
            (is (= msg (:msg env-primary)))
            (is (= msg (:msg env-data))))
          (finally
            (try
              (.close admin-primary)
              (catch Exception _e nil))
            (try
              (.close admin-data)
              (catch Exception _e nil))
            (kc/close-consumer! consumer-primary)
            (kc/close-consumer! consumer-data)
            (kc/close! client-primary)
            (kc/close! client-data)))))))
