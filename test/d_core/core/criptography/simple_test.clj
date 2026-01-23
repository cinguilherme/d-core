(ns d-core.core.criptography.simple-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.criptography.protocol :as protocol]
            [d-core.core.criptography.simple :as simple])
  (:import (javax.crypto.spec SecretKeySpec)))

(defn- secret-key
  [^bytes bytes]
  (SecretKeySpec. bytes "AES"))

(deftest simple-cryptography-roundtrip
  (testing "encrypt/decrypt roundtrip"
    (let [key (secret-key (.getBytes "0123456789ABCDEF" "UTF-8"))
          crypto (simple/->SimpleCriptography key)
          payload "hello-world"
          encrypted (protocol/encrypt crypto payload)
          decrypted (protocol/decrypt crypto encrypted)]
      (is (instance? (class (byte-array 0)) encrypted))
      (is (= payload decrypted)))))
