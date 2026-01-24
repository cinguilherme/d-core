(ns d-core.core.codecs
  ;; Integrant requires `d-core.core.codecs` for keys like
  ;; :d-core.core.codecs/edn and :d-core.core.codecs/json.
  (:require [d-core.core.codecs.edn]
            [d-core.core.codecs.json]
            [d-core.core.codecs.bytes]
            [d-core.core.codecs.byte-buffer]
            [d-core.core.codecs.protocol]))
