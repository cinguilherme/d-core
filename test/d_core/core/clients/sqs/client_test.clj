(ns d-core.core.clients.sqs.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.sqs.client :as sqs])
  (:import (com.amazonaws.services.sqs.model Message MessageAttributeValue)))

(deftest local-endpoint-detection
  (testing "local-endpoint? detects localhost targets"
    (is (true? (#'sqs/local-endpoint? "http://localhost:9324")))
    (is (true? (#'sqs/local-endpoint? "http://127.0.0.1:9324")))
    (is (false? (#'sqs/local-endpoint? "https://sqs.us-east-1.amazonaws.com")))))

(deftest message-map-shape
  (testing "message->map preserves key fields"
    (let [attr (doto (MessageAttributeValue.)
                 (.setDataType "String")
                 (.setStringValue "abc"))
          msg (doto (Message.)
                (.setMessageId "m-1")
                (.setReceiptHandle "r-1")
                (.setBody "body")
                (.setAttributes {"ApproximateReceiveCount" "1"})
                (.setMessageAttributes {"trace-id" attr}))
          m (#'sqs/message->map msg)]
      (is (= "m-1" (:message-id m)))
      (is (= "r-1" (:receipt-handle m)))
      (is (= "body" (:body m)))
      (is (= {"ApproximateReceiveCount" "1"} (:attributes m)))
      (is (= {:data-type "String" :string-value "abc"}
             (get-in m [:message-attributes "trace-id"]))))))
