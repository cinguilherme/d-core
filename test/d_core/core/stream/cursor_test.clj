(ns d-core.core.stream.cursor-test
  (:require [clojure.test :refer [deftest is testing]]
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
