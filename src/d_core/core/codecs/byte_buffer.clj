(ns d-core.core.codecs.byte-buffer
  (:require [integrant.core :as ig]
            [d-core.core.codecs.protocol :as codec]))

(defn- byte-buffer->bytes
  [^java.nio.ByteBuffer buf]
  (let [dup (.duplicate buf)
        arr (byte-array (.remaining dup))]
    (.get dup arr)
    arr))

(defrecord ByteBufferCodec []
  codec/Codec
  (encode [_ value]
    (cond
      (instance? java.nio.ByteBuffer value) (byte-buffer->bytes value)
      (bytes? value) value
      (string? value) (.getBytes ^String value "UTF-8")
      :else (throw (ex-info "ByteBuffer codec expected ByteBuffer, bytes, or string"
                            {:value-type (type value)}))))
  (decode [_ payload]
    (cond
      (instance? java.nio.ByteBuffer payload) payload
      (bytes? payload) (java.nio.ByteBuffer/wrap ^bytes payload)
      (string? payload) (java.nio.ByteBuffer/wrap (.getBytes ^String payload "UTF-8"))
      :else (throw (ex-info "ByteBuffer codec expected ByteBuffer, bytes, or string payload"
                            {:payload-type (type payload)})))))

(defmethod ig/init-key :d-core.core.codecs/byte-buffer
  [_ _opts]
  (->ByteBufferCodec))
