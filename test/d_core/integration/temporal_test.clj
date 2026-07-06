(ns d-core.integration.temporal-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.temporal.client :as temporal]))

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

(deftest temporal-client-connectivity
  (testing "Temporal client reaches a live Temporal frontend"
    (if-not (integration-enabled?)
      (is true "Skipping Temporal integration test; set INTEGRATION=1")
      (let [client (temporal/make-client {:target (temporal-target)
                                          :namespace (temporal-namespace)
                                          :connect-timeout-ms 10000
                                          :rpc-timeout-ms 5000
                                          :system-info-timeout-ms 5000})
            health (wait-for-up client 30000)]
        (try
          (is (= :up (:status health))
              (str "Temporal frontend should become reachable: " health))
          (is (seq (get-in health [:system-info :server-version])))
          (let [cluster (temporal/cluster-info client)]
            (is (seq (:server-version cluster)))
            (is (pos? (:history-shard-count cluster))))
          (finally
            (temporal/close-now! client)))))))
