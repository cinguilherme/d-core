(ns d-core.libs.temporal-worker-integration-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [d-core.libs.workers :as workers]
            [d-core.libs.temporal-worker] ; ensure it's loaded
            [clojure.core.async :as async])
  (:import [io.temporal.client WorkflowClient WorkflowOptions WorkflowStub]
           [io.temporal.serviceclient WorkflowServiceStubs WorkflowServiceStubsOptions]
           [io.temporal.activity DynamicActivity]
           [io.temporal.workflow DynamicWorkflow]
           [java.time Duration]))

;; Integration test requires a running Temporal instance on localhost:7233
(def run-integration? (Boolean/parseBoolean (System/getenv "INTEGRATION")))

;; A dynamic workflow class using deftype so it can be instantiated by Temporal
(deftype MyWorkflowImpl []
  DynamicWorkflow
  (execute [_ args]
    (let [name (.get args 0 String)]
      (str "Hello Workflow " name))))

(deftest test-temporal-worker-integration
  (when run-integration?
    (testing "Temporal worker correctly registers and executes a dynamic workflow"
      (let [service (WorkflowServiceStubs/newLocalServiceStubs)
            client  (WorkflowClient/newInstance service)
            task-queue "test-queue"
            
            ;; A dynamic activity
            my-activity (reify DynamicActivity
                          (execute [_ args]
                            (let [name (.get args 0 String)]
                              (str "Hello Activity " name))))
            
            definition {:channels {}
                        :workers {:my-temporal-worker
                                  {:kind :temporal
                                   :task-queue task-queue
                                   ;; We pass the Class of the workflow to register
                                   :workflows [MyWorkflowImpl]}}}
            
            components {:temporal-client client}
            
            ;; Start the worker
            system (workers/start-workers definition components)
            
            ;; Create an untyped workflow stub to trigger the workflow
            options (-> (WorkflowOptions/newBuilder)
                        (.setTaskQueue task-queue)
                        (.setWorkflowRunTimeout (Duration/ofSeconds 10))
                        (.build))
            stub (.newUntypedWorkflowStub client "DynamicWorkflow" options)]
            
        (try
          ;; Start the workflow execution with a single argument "Temporal"
          (.start stub (into-array Object ["Temporal"]))
          
          ;; Wait for the result
          (let [result (.getResult stub String)]
            (is (= "Hello Workflow Temporal" result)))
            
          (finally
            ;; Stop the worker system
            ((:stop! system))
            (.shutdown service)))))))
