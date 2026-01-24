(ns d-core.core.compression.protocol
  (:require [integrant.core :as ig]))

(defprotocol CompressionProtocol
  (compress [compressor bytes] "Compress a byte array.")
  (decompress [compressor bytes] "Decompress a byte array."))

(defmethod ig/init-key :d-core.core.compression/compressor
  [_ {:keys [compressor]}]
  (if compressor
    compressor
    (throw (ex-info "Compression component requires :compressor" {}))))
