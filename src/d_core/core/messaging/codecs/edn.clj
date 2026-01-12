(ns d-core.core.messaging.codecs.edn
  (:require [clojure.edn :as edn]
            [integrant.core :as ig]
            [d-core.core.messaging.codec :as codec]))

(defrecord EdnCodec []
  codec/Codec
  (encode [_ value]
    (pr-str value))
  (decode [_ payload]
    (cond
      (string? payload) (edn/read-string payload)
      (bytes? payload) (edn/read-string (String. ^bytes payload "UTF-8"))
      :else (throw (ex-info "EDN codec expected string or bytes payload"
                            {:payload-type (type payload)})))))

(defmethod ig/init-key :d-core.core.messaging.codecs/edn
  [_ _opts]
  (->EdnCodec))

