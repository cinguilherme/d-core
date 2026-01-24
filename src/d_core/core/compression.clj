(ns d-core.core.compression
  ;; Integrant requires `d-core.core.compression` for keys like
  ;; :d-core.core.compression/gzip.
  (:require [d-core.core.compression.gzip]
            [d-core.core.compression.protocol]))
