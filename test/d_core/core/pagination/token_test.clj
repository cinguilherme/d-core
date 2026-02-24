(ns d-core.core.pagination.token-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.pagination.token :as token]))

(deftest encode-decode-roundtrip
  (testing "encodes and decodes a map token"
    (let [value {:source "redis" :cursor "1-0" :direction "backward"}
          encoded (token/encode-token value)]
      (is (string? encoded))
      (is (= value (token/decode-token encoded))))))

(deftest decode-invalid-token
  (testing "returns nil for blank or invalid token"
    (is (nil? (token/decode-token nil)))
    (is (nil? (token/decode-token "")))
    (is (nil? (token/decode-token "%%%invalid%%%")))))

(deftest decode-token-map-boundary
  (testing "decode-token-map returns map payloads and rejects non-map payloads"
    (let [map-token (token/encode-token {:a 1})
          vec-token (token/encode-token [1 2 3])]
      (is (= {:a 1} (token/decode-token-map map-token)))
      (is (nil? (token/decode-token-map vec-token)))
      (is (nil? (token/decode-token-map "%%%invalid%%%"))))))
