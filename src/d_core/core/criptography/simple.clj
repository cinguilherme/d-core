(ns d-core.core.criptography.simple
  (:require [d-core.core.criptography.protocol :as protocol]
            [integrant.core :as ig]))

(defn encrypt-value [key value]
  (let [cipher (javax.crypto.Cipher/getInstance "AES")]
    (doto cipher
      (.init javax.crypto.Cipher/ENCRYPT_MODE key))
    (.doFinal cipher (.getBytes value))))

(defn decrypt-value [key value]
  (let [cipher (javax.crypto.Cipher/getInstance "AES")]
    (doto cipher
      (.init javax.crypto.Cipher/DECRYPT_MODE key))
    (String. (.doFinal cipher value))))

(defrecord SimpleCriptography [key]
  protocol/CriptographyProtocol
  (encrypt [_ value] (encrypt-value key value))
  (decrypt [_ value] (decrypt-value key value)))

(defmethod ig/init-key :d-core.core.criptography/simple
  [_ {:keys [key]}]
  (->SimpleCriptography key))