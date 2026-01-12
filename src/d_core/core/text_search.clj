(ns d-core.core.text-search
  (:require [integrant.core :as ig]
            [d-core.core.text-search.common :as impl]))

(defmethod ig/init-key :d-core.core.text-search/common
  [_ opts]
  (impl/init-common opts))

