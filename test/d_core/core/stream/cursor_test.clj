(ns d-core.core.stream.cursor-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.pagination.token :as token]
            [d-core.core.stream.cursor :as cursor]))

(deftest encode-decode-cursor
  (testing "encodes and decodes source-aware cursor token"
    (let [encoded (cursor/encode-cursor {:conversation-id "c-1"
                                         :cursor "10-0"
                                         :direction :backward
                                         :source :redis})
          decoded (cursor/decode-cursor encoded)]
      (is (= :redis (:source decoded)))
      (is (= :backward (:direction decoded)))
      (is (= "10-0" (:cursor decoded)))
      (is (= "c-1" (:conversation-id decoded))))))

(deftest decode-seq-cursor
  (testing "numeric cursor is exposed as seq-cursor"
    (let [encoded (cursor/encode-cursor {:conversation-id "c-2"
                                         :cursor 42
                                         :direction :backward
                                         :source :minio})
          decoded (cursor/decode-cursor encoded)]
      (is (= :minio (:source decoded)))
      (is (= 42 (:cursor decoded)))
      (is (= 42 (:seq-cursor decoded))))))

(deftest decode-invalid-cursor-token
  (testing "invalid base64/json token is marked invalid"
    (let [decoded (cursor/decode-cursor "%%%invalid%%")]
      (is (true? (:invalid? decoded)))
      (is (nil? (:token decoded)))))

  (testing "non-map token payload is marked invalid"
    (let [vector-token (token/encode-token [1 2 3])
          decoded (cursor/decode-cursor vector-token)]
      (is (true? (:invalid? decoded)))))

  (testing "unknown source or invalid cursor type is marked invalid"
    (let [bad-source (token/encode-token {:conversation_id "c-1"
                                          :cursor "1-0"
                                          :direction "backward"
                                          :source "other"})
          bad-type (token/encode-token {:conversation_id "c-1"
                                        :cursor 123
                                        :direction "backward"
                                        :source "redis"})]
      (is (true? (:invalid? (cursor/decode-cursor bad-source))))
      (is (true? (:invalid? (cursor/decode-cursor bad-type)))))))
