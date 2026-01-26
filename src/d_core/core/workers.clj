(ns d-core.core.workers
  (:require [d-core.libs.workers :as workers]
            [integrant.core :as ig]))

(defmethod ig/init-key :d-core.core.workers/system
  [_ {:keys [definition components dev-guard? guard-ms]
      :or {components {}}}]
  (when-not definition
    (throw (ex-info "Workers system requires :definition" {})))
  (workers/start-workers definition components {:dev-guard? dev-guard?
                                                :guard-ms guard-ms}))

(defmethod ig/halt-key! :d-core.core.workers/system
  [_ system]
  (when-let [stop! (:stop! system)]
    (stop!)))
