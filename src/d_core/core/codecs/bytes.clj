(ns d-core.core.codecs.bytes
  (:require [integrant.core :as ig]
            [d-core.core.codecs.protocol :as codec]))

(defn- byte-buffer->bytes
  [^java.nio.ByteBuffer buf]
  (let [dup (.duplicate buf)
        arr (byte-array (.remaining dup))]
    (.get dup arr)
    arr))

(defrecord BytesCodec []
  codec/Codec
  (encode [_ value]
    (cond
      (bytes? value) value
      (instance? java.nio.ByteBuffer value) (byte-buffer->bytes value)
      (string? value) (.getBytes ^String value "UTF-8")
      :else (throw (ex-info "Bytes codec expected bytes, ByteBuffer, or string"
                            {:value-type (type value)}))))
  (decode [_ payload]
    (cond
      (bytes? payload) payload
      (instance? java.nio.ByteBuffer payload) (byte-buffer->bytes payload)
      (string? payload) (.getBytes ^String payload "UTF-8")
      :else (throw (ex-info "Bytes codec expected bytes, ByteBuffer, or string payload"
                            {:payload-type (type payload)})))))

(defmethod ig/init-key :d-core.core.codecs/bytes
  [_ _opts]
  (->BytesCodec))
