(ns d-core.integration.sqs-client-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.sqs.client :as sqs])
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

(deftest sqs-client-basic-ops
  (testing "create/send/receive/delete roundtrip"
    (if-not (integration-enabled?)
      (is true "Skipping SQS integration test; set INTEGRATION=1")
      (let [client (sqs/make-client (sqs-opts))
            queue (str "dcore.int.sqs." (UUID/randomUUID))
            queue-url (sqs/create-queue! client queue {})
            body (str "hello-" (UUID/randomUUID))]
        (try
          (let [send-res (sqs/send-message! client queue-url body {})
                messages (sqs/receive-messages! client queue-url {:max-messages 1 :wait-seconds 1})
                msg (first messages)]
            (is (string? (:message-id send-res)))
            (is (some? msg))
            (is (= body (:body msg)))
            (sqs/delete-message! client queue-url (:receipt-handle msg)))
          (finally
            (sqs/delete-queue! client queue-url)))))))
