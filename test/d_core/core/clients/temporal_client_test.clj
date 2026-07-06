(ns d-core.core.clients.temporal-client-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.temporal.client :as temporal])
  (:import [io.temporal.client WorkflowOptions]
           [io.temporal.serviceclient WorkflowServiceStubsOptions]))

(deftest build-service-options-uses-local-defaults
  (let [^WorkflowServiceStubsOptions options (temporal/build-service-options {})]
    (is (= temporal/default-target (.getTarget options)))))

(deftest build-client-options-uses-default-namespace
  (let [options (temporal/build-client-options {})]
    (is (= temporal/default-namespace (.getNamespace options)))))

(deftest build-workflow-options-coerces-common-fields
  (let [^WorkflowOptions options (temporal/build-workflow-options
                                  {:task-queue :jobs
                                   :workflow-id :job-1
                                   :workflow-run-timeout-ms 1000})]
    (is (= "jobs" (.getTaskQueue options)))
    (is (= "job-1" (.getWorkflowId options)))
    (is (= 1000 (.toMillis (.getWorkflowRunTimeout options))))))

(deftest make-client-can-build-lazy-local-handle
  (let [client (temporal/make-client {})]
    (try
      (is (= temporal/default-target (:target client)))
      (is (= temporal/default-namespace (:namespace client)))
      (is (some? (:service-stubs client)))
      (is (some? (:workflow-client client)))
      (finally
        (temporal/close-now! client)))))

(deftest invalid-timeout-fields-fail-fast
  (testing "negative durations are rejected before Java builders are called"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"greater than zero"
         (temporal/build-workflow-options {:workflow-run-timeout-ms -1})))))
