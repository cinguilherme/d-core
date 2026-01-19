(ns d-core.dev.xtdb-playground
  (:require [xtdb.api :as xt]
            [xtdb.node :as xtn]))

(with-open [node (xtn/start-node)]
  (xt/status node)

  ;; ...
  )