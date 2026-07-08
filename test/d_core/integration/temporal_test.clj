(ns d-core.integration.temporal-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.temporal.client :as temporal])
  (:import [java.util UUID]))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_TEMPORAL"))))

(defn- temporal-target
  []
  (or (System/getenv "DCORE_TEMPORAL_TARGET")
      "localhost:7233"))

(defn- temporal-namespace
  []
  (or (System/getenv "DCORE_TEMPORAL_NAMESPACE")
      "default"))

(defn- wait-for-up
  [client timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [result (temporal/health client)]
        (cond
          (= :up (:status result)) result
          (< (System/currentTimeMillis) deadline) (do (Thread/sleep 250) (recur))
          :else result)))))

(defn- make-client
  []
  (temporal/make-client {:target (temporal-target)
                         :namespace (temporal-namespace)
                         :connect-timeout-ms 10000
                         :rpc-timeout-ms 5000
                         :system-info-timeout-ms 5000}))

(deftest temporal-client-connectivity
  (testing "Temporal client reaches a live Temporal frontend"
    (if-not (integration-enabled?)
      (is true "Skipping Temporal integration test; set INTEGRATION=1")
      (let [client (make-client)
            health (wait-for-up client 30000)]
        (try
          (is (= :up (:status health))
              (str "Temporal frontend should become reachable: " health))
          (when (= :up (:status health))
            (is (seq (get-in health [:system-info :server-version])))
            (let [cluster (temporal/cluster-info client)]
              (is (seq (:server-version cluster)))
              (is (pos? (:history-shard-count cluster)))))
          (finally
            (temporal/close-now! client)))))))

(deftest temporal-untyped-workflow-start-and-terminate
  (testing "Temporal accepts an untyped workflow execution and lets the client terminate it"
    (if-not (integration-enabled?)
      (is true "Skipping Temporal workflow integration test; set INTEGRATION=1")
      (let [client (make-client)
            workflow-id (str "dcore-int-" (UUID/randomUUID))
            task-queue (str "dcore-int-temporal-" (UUID/randomUUID))
            stub (temporal/new-untyped-workflow-stub
                  client
                  "DCoreIntegrationWorkflow"
                  {:task-queue task-queue
                   :workflow-id workflow-id
                   :workflow-run-timeout-ms 60000
                   :workflow-task-timeout-ms 5000})]
        (try
          (let [health (wait-for-up client 30000)]
            (is (= :up (:status health))
                (str "Temporal frontend should become reachable: " health))
            (when (= :up (:status health))
              (let [execution (.start stub (object-array []))
                    execution-map (temporal/workflow-execution->map execution)]
                (is (= workflow-id (:workflow-id execution-map)))
                (is (seq (:run-id execution-map))))))
          (finally
            (try
              (.terminate stub "d-core integration cleanup" (object-array []))
              (catch Exception _e
                nil))
            (temporal/close-now! client)))))))
