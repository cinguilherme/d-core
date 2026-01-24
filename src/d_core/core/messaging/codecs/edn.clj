(ns d-core.core.messaging.codecs.edn
  (:require [integrant.core :as ig]
            [d-core.core.codecs.edn :as edn-codec]))

(defmethod ig/init-key :d-core.core.messaging.codecs/edn
  [_ _opts]
  (edn-codec/->EdnCodec))
