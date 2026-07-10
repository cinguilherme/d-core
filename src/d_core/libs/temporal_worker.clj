(ns d-core.libs.temporal-worker
  (:require [d-core.libs.workers :as workers]
            [clojure.core.async :as async])
  (:import [io.temporal.worker WorkerFactory Worker]))

(set! *warn-on-reflection* true)

(defmethod workers/start-worker-by-kind :temporal
  [worker ctx opts]
  (let [{:keys [task-queue activities workflows]} worker
        client (get-in ctx [:components :temporal-client])
        factory (WorkerFactory/newInstance client)
        ^Worker temporal-worker (.newWorker factory task-queue)]
    
    (when (seq activities)
      (let [^"[Ljava.lang.Object;" act-array (into-array Object activities)]
        (.registerActivitiesImplementations temporal-worker act-array)))
      
    (when (seq workflows)
      (let [^"[Ljava.lang.Class;" wf-array (into-array Class workflows)]
        (.registerWorkflowImplementationTypes temporal-worker wf-array)))
      
    (.start factory)
    
    (async/go
      (async/<! (:stop-chan opts))
      (.shutdown factory))))
