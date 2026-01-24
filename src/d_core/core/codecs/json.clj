(ns d-core.core.codecs.json
  (:require [integrant.core :as ig]
            [cheshire.core :as json]
            [d-core.core.codecs.protocol :as codec]))

(defrecord JsonCodec []
  codec/Codec
  (encode [_ value]
    (json/generate-string value))
  (decode [_ value]
    (json/parse-string value keyword)))

(defmethod ig/init-key :d-core.core.codecs/json
  [_ _opts]
  (->JsonCodec))
