(ns d-core.core.criptography.storage-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.criptography.protocol :as protocol]
            [d-core.core.criptography.storage]
            [d-core.core.storage.protocol :as storage]
            [integrant.core :as ig])
  (:import (java.util Base64)))

(defn- in-memory-storage
  [data]
  (reify storage/StorageProtocol
    (storage-get [_ key _opts]
      (if-let [v (get data key)]
        {:ok true :value v}
        {:ok false :error-type :not-found}))
    (storage-put [_ _ _ _] (throw (ex-info "storage-put not supported in test" {})))
    (storage-delete [_ _ _] (throw (ex-info "storage-delete not supported in test" {})))))

(deftest storage-cryptography-roundtrip
  (testing "encrypt/decrypt roundtrip from storage key material"
    (let [key-bytes (.getBytes "0123456789ABCDEF" "UTF-8")
          key-b64 (.encodeToString (Base64/getEncoder) key-bytes)
          storage (in-memory-storage {"keys/app.key" key-b64})
          crypto (ig/init-key :d-core.core.criptography/storage
                              {:storage storage
                               :key-path "keys/app.key"})
          payload "secret-envelope"
          enc1 (protocol/encrypt crypto payload)
          enc2 (protocol/encrypt crypto payload)
          dec1 (protocol/decrypt crypto enc1)
          dec2 (protocol/decrypt crypto enc2)]
      (is (= payload dec1))
      (is (= payload dec2))
      (is (not= (seq enc1) (seq enc2)) "GCM should randomize ciphertext via IV"))))
