(ns d-core.core.compression.gzip
  (:require [integrant.core :as ig]
            [d-core.core.compression.protocol :as p])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (java.util.zip GZIPInputStream GZIPOutputStream)))

(defrecord GzipCompression []
  p/CompressionProtocol
  (compress [_ bytes]
    (let [out (ByteArrayOutputStream.)]
      (with-open [gzip (GZIPOutputStream. out)]
        (.write gzip ^bytes bytes))
      (.toByteArray out)))
  (decompress [_ bytes]
    (with-open [in (GZIPInputStream. (ByteArrayInputStream. ^bytes bytes))
                out (ByteArrayOutputStream.)]
      (let [buf (byte-array 4096)]
        (loop [n (.read in buf)]
          (when (pos? n)
            (.write out buf 0 n)
            (recur (.read in buf)))))
      (.toByteArray out))))

(defmethod ig/init-key :d-core.core.compression/gzip
  [_ _opts]
  (->GzipCompression))
