(ns d-core.core.messaging.codec
  (:require [integrant.core :as ig]
            [d-core.core.codecs.protocol :as codec]))

(def Codec codec/Codec)
(def encode codec/encode)
(def decode codec/decode)
(def ->PipelineCodec codec/->PipelineCodec)

(defmethod ig/init-key :d-core.core.messaging/codec
  [_ {:keys [codecs codec]
      :or {codecs nil}}]
  ;; Accept either:
  ;; - {:codec <single-codec>} or
  ;; - {:codecs [<codec1> <codec2> ...]} for a pipeline.
  (cond
    (sequential? codecs) (codec/->PipelineCodec codecs)
    codec codec
    :else (throw (ex-info "Codec component requires :codec or :codecs" {}))))
