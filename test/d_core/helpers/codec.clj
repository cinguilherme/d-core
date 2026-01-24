(ns d-core.helpers.codec
  (:require [d-core.core.codecs.protocol :as codec]))

;; Test fixtures and helpers

(defn make-test-codec
  "Returns a codec that can be configured to succeed or fail decode."
  [decode-fn]
  (reify codec/Codec
    (decode [_ payload]
      (decode-fn payload))
    (encode [_ value]
      (.getBytes (pr-str value) "UTF-8"))))
