(ns d-core.core.ai
  (:require [d-core.core.ai.common :as impl]
            [integrant.core :as ig]))

(defmethod ig/init-key :d-core.core.ai/common
  [_ opts]
  (impl/init-common opts))
