(ns d-core.core.messaging.codecs
  ;; Integrant requires `d-core.core.messaging.codecs` for keys like
  ;; :d-core.core.messaging.codecs/edn and :d-core.core.messaging.codecs/json.
  ;; This namespace exists to load the concrete codec implementations.
  (:require [d-core.core.messaging.codecs.edn]
            [d-core.core.messaging.codecs.json]))

