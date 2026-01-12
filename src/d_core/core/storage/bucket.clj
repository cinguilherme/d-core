(ns d-core.core.storage.bucket
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defmethod ig/init-key :d-core.core.storage.bucket/bucket
  [_ {:keys [logger]}]
  (logger/log logger :info ::initializing-bucket)
  (println "Initializing Bucket"))

(defmethod ig/halt-key! :d-core.core.storage.bucket/bucket
  [_ {:keys [logger]}]
  (logger/log logger :info ::stopping-bucket)
  (println "Stopping Bucket"))