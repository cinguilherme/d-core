(ns d-core.core.messaging.codecs.json
  (:require [integrant.core :as ig]
            [d-core.core.codecs.json :as json-codec]))

(defmethod ig/init-key :d-core.core.messaging.codecs/json
  [_ _opts]
  (json-codec/->JsonCodec))
