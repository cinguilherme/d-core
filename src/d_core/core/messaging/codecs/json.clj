(ns d-core.core.messaging.codecs.json
  (:require [integrant.core :as ig] 
            [d-core.core.messaging.codec :as codec]
            [cheshire.core :as json]))

(defrecord JsonCodec []
  codec/Codec
  (encode [_ value] (json/generate-string value))
  (decode [_ value] (json/parse-string value keyword)))

(defmethod ig/init-key :d-core.core.messaging.codecs/json
  [_ _opts]
  (->JsonCodec))