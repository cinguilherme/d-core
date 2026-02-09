;; More realistic cryptography backed by external storage for key material.
(ns d-core.core.criptography.storage
  (:require [d-core.core.criptography.protocol :as protocol]
            [d-core.core.storage.protocol :as storage]
            [integrant.core :as ig])
  (:import (javax.crypto Cipher)
           (javax.crypto.spec GCMParameterSpec SecretKeySpec)
           (java.security SecureRandom)
           (java.util Base64)))

(def ^:private byte-array-class (class (byte-array 0)))
(def ^:private gcm-iv-bytes 12)
(def ^:private gcm-tag-bits 128)

(defn- bytes?
  [value]
  (instance? byte-array-class value))

(defn- random-bytes
  [n]
  (let [bytes (byte-array n)
        rng (SecureRandom.)]
    (.nextBytes rng bytes)
    bytes))

(defn- concat-bytes
  [^bytes a ^bytes b]
  (let [out (byte-array (+ (alength a) (alength b)))]
    (System/arraycopy a 0 out 0 (alength a))
    (System/arraycopy b 0 out (alength a) (alength b))
    out))

(defn- split-iv
  [^bytes data]
  (let [len (alength data)]
    (when (< len (inc gcm-iv-bytes))
      (throw (ex-info "Encrypted payload too short to contain IV"
                      {:len len :iv-bytes gcm-iv-bytes})))
    (let [iv (byte-array gcm-iv-bytes)
          payload (byte-array (- len gcm-iv-bytes))]
      (System/arraycopy data 0 iv 0 gcm-iv-bytes)
      (System/arraycopy data gcm-iv-bytes payload 0 (alength payload))
      [iv payload])))

(defn- decode-key-material
  [key-material encoding]
  (cond
    (bytes? key-material) key-material
    (string? key-material)
    (case encoding
      :base64 (.decode (Base64/getDecoder) ^String key-material)
      :utf8 (.getBytes ^String key-material "UTF-8")
      (throw (ex-info "Unsupported key encoding"
                      {:encoding encoding})))
    :else (throw (ex-info "Unsupported key material type"
                          {:type (type key-material)}))))

(defn- load-key
  [storage key-path encoding algorithm opts]
  (let [result (storage/storage-get storage key-path opts)]
    (when-not (:ok result)
      (throw (ex-info
               (if (= :not-found (:error-type result))
                 "Key material not found in storage"
                 (str "Failed to load key material from storage: "
                      (:error result)))
               (merge {:key-path key-path} (dissoc result :ok)))))
    (SecretKeySpec. (decode-key-material (:value result) encoding) algorithm)))

(defn encrypt-value
  [key value]
  (let [cipher (Cipher/getInstance "AES/GCM/NoPadding")
        iv (random-bytes gcm-iv-bytes)
        spec (GCMParameterSpec. gcm-tag-bits iv)]
    (.init cipher Cipher/ENCRYPT_MODE key spec)
    (let [plaintext (.getBytes (str value) "UTF-8")
          ciphertext (.doFinal cipher plaintext)]
      (concat-bytes iv ciphertext))))

(defn decrypt-value
  [key value]
  (let [[iv payload] (split-iv value)
        cipher (Cipher/getInstance "AES/GCM/NoPadding")
        spec (GCMParameterSpec. gcm-tag-bits iv)]
    (.init cipher Cipher/DECRYPT_MODE key spec)
    (String. (.doFinal cipher payload) "UTF-8")))

(defrecord StorageCriptography [key]
  protocol/CriptographyProtocol
  (encrypt [_ value] (encrypt-value key value))
  (decrypt [_ value] (decrypt-value key value)))

(defmethod ig/init-key :d-core.core.criptography/storage
  [_ {:keys [storage key-path encoding algorithm storage-opts]
      :or {encoding :base64
           algorithm "AES"
           storage-opts {}}}]
  (when-not storage
    (throw (ex-info "Storage-backed cryptography requires :storage" {})))
  (when-not key-path
    (throw (ex-info "Storage-backed cryptography requires :key-path" {})))
  (->StorageCriptography (load-key storage key-path encoding algorithm storage-opts)))
