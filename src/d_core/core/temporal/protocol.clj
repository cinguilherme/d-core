(ns d-core.core.temporal.protocol)

(defprotocol TemporalOperations
  (temporal-health [this opts]
    "Returns a low-level health map for a Temporal connection.")
  (temporal-start-workflow! [this workflow-type opts args]
    "Starts an untyped Workflow Execution. Runtime-level mapping decisions stay with callers."))
