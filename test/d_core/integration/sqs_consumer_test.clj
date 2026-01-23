(ns d-core.integration.sqs-consumer-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.clients.sqs.client :as sqs]
            [d-core.core.consumers.sqs :as sqs-consumer]
            [d-core.core.messaging.codecs.edn :as edn-codec]
            [d-core.core.messaging.codec :as codec])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_SQS"))))

(defn- sqs-opts
  []
  {:endpoint (or (System/getenv "DCORE_SQS_ENDPOINT") "http://localhost:9324")
   :region (or (System/getenv "DCORE_SQS_REGION") "us-east-1")
   :access-key (or (System/getenv "DCORE_SQS_ACCESS_KEY") "x")
   :secret-key (or (System/getenv "DCORE_SQS_SECRET_KEY") "x")})

(defn- wait-for
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 10) (recur))
          false)))))

(deftest sqs-consumer-handles-messages
  (testing "SQS runtime consumes and deletes messages"
    (if-not (integration-enabled?)
      (is true "Skipping SQS integration test; set INTEGRATION=1")
      (let [client (sqs/make-client (sqs-opts))
            queue (str "dcore.int.sqs-consumer." (UUID/randomUUID))
            queue-url (sqs/create-queue! client queue {})
            handled (atom [])
            handler (fn [envelope] (swap! handled conj envelope))
            codec-instance (edn-codec/->EdnCodec)
            logger (:logger (h-logger/make-test-logger))
            routing {:topics {:orders {:queue queue}}
                     :subscriptions {:sub {:topic :orders
                                           :handler handler
                                           :source :sqs
                                           :options {:queue-url queue-url
                                                     :wait-seconds 1
                                                     :max-messages 1
                                                     :idle-sleep-ms 50}}}}
            runtime (ig/init-key :d-core.core.consumers.sqs/runtime
                                 {:sqs client
                                  :routing routing
                                  :codec codec-instance
                                  :logger logger})]
        (try
          (let [payload {:msg {:id 1 :status "ok"}}
                body (codec/encode codec-instance payload)]
            (sqs/send-message! client queue-url body {})
            (is (wait-for #(= 1 (count @handled)) 2000))
            (is (= payload (first @handled))))
          (finally
            (ig/halt-key! :d-core.core.consumers.sqs/runtime runtime)
            (sqs/delete-queue! client queue-url)))))))
