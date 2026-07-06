(ns d-core.core.clients.temporal.client
  (:require [clojure.string :as str]
            [d-core.core.temporal.protocol :as temporal-protocol])
  (:import [io.temporal.api.common.v1 WorkflowExecution]
           [io.temporal.api.workflowservice.v1 GetClusterInfoRequest GetSystemInfoRequest
            WorkflowServiceGrpc$WorkflowServiceBlockingStub]
           [io.temporal.client WorkflowClient WorkflowClientOptions WorkflowOptions WorkflowStub]
           [io.temporal.serviceclient WorkflowServiceStubs WorkflowServiceStubsOptions]
           [java.time Duration]
           [java.util.concurrent TimeUnit]))

(def default-target
  "localhost:7233")

(def default-namespace
  "default")

(def default-connect-timeout-ms
  5000)

(declare health start-untyped-workflow!)

(defrecord TemporalClient [^WorkflowServiceStubs service-stubs
                           ^WorkflowClient workflow-client
                           target
                           namespace
                           options]
  temporal-protocol/TemporalOperations
  (temporal-health [this _opts]
    (health this))
  (temporal-start-workflow! [this workflow-type opts args]
    (apply start-untyped-workflow! this workflow-type opts args))

  Object
  (toString [_]
    (str "#TemporalClient{:target " (pr-str target)
         ", :namespace " (pr-str namespace) "}")))

(defn- non-blank-string
  [value field default-value]
  (let [value (str/trim (str (or value default-value)))]
    (when (str/blank? value)
      (throw (ex-info "Temporal client field must be non-blank"
                      {:type ::invalid-field
                       :field field
                       :value value})))
    value))

(defn- positive-long
  [value field]
  (when value
    (when-not (instance? Number value)
      (throw (ex-info "Temporal client field must be a positive integer"
                      {:type ::invalid-field
                       :field field
                       :value value})))
    (let [v (long value)]
      (when (<= v 0)
        (throw (ex-info "Temporal client field must be greater than zero"
                        {:type ::invalid-field
                         :field field
                         :value value})))
      v)))

(defn- ->string
  [value]
  (cond
    (nil? value) nil
    (keyword? value) (name value)
    :else (str value)))

(defn- duration
  ^Duration
  [value field]
  (some-> value (positive-long field) Duration/ofMillis))

(defn- maybe-configure
  [builder configure]
  (if configure
    (or (configure builder) builder)
    builder))

(defn build-service-options
  ^WorkflowServiceStubsOptions
  [{:keys [target enable-https? disable-health-check? rpc-timeout-ms rpc-long-poll-timeout-ms
           rpc-query-timeout-ms query-rpc-timeout-ms system-info-timeout-ms
           health-check-timeout-ms health-check-attempt-timeout-ms headers ssl-context
           metrics-scope grpc-metadata-providers grpc-client-interceptors configure-service-options]}]
  (let [target (non-blank-string target :target default-target)
        builder (WorkflowServiceStubsOptions/newBuilder)]
    (.setTarget builder target)
    (when (some? enable-https?)
      (.setEnableHttps builder (boolean enable-https?)))
    (when (some? disable-health-check?)
      (.setDisableHealthCheck builder (boolean disable-health-check?)))
    (when-let [v (duration rpc-timeout-ms :rpc-timeout-ms)]
      (.setRpcTimeout builder v))
    (when-let [v (duration rpc-long-poll-timeout-ms :rpc-long-poll-timeout-ms)]
      (.setRpcLongPollTimeout builder v))
    (when-let [v (duration rpc-query-timeout-ms :rpc-query-timeout-ms)]
      (.setRpcQueryTimeout builder v))
    (when-let [v (duration query-rpc-timeout-ms :query-rpc-timeout-ms)]
      (.setQueryRpcTimeout builder v))
    (when-let [v (duration system-info-timeout-ms :system-info-timeout-ms)]
      (.setSystemInfoTimeout builder v))
    (when-let [v (duration health-check-timeout-ms :health-check-timeout-ms)]
      (.setHealthCheckTimeout builder v))
    (when-let [v (duration health-check-attempt-timeout-ms :health-check-attempt-timeout-ms)]
      (.setHealthCheckAttemptTimeout builder v))
    (when headers
      (.setHeaders builder headers))
    (when ssl-context
      (.setSslContext builder ssl-context))
    (when metrics-scope
      (.setMetricsScope builder metrics-scope))
    (when grpc-metadata-providers
      (.setGrpcMetadataProviders builder grpc-metadata-providers))
    (when grpc-client-interceptors
      (.setGrpcClientInterceptors builder grpc-client-interceptors))
    (.build (maybe-configure builder configure-service-options))))

(defn build-client-options
  ^WorkflowClientOptions
  [{:keys [namespace identity binary-checksum context-propagators data-converter interceptors
           configure-client-options]}]
  (let [namespace (non-blank-string namespace :namespace default-namespace)
        builder (WorkflowClientOptions/newBuilder)]
    (.setNamespace builder namespace)
    (when identity
      (.setIdentity builder (->string identity)))
    (when binary-checksum
      (.setBinaryChecksum builder (->string binary-checksum)))
    (when context-propagators
      (.setContextPropagators builder context-propagators))
    (when data-converter
      (.setDataConverter builder data-converter))
    (when interceptors
      (.setInterceptors builder interceptors))
    (.build (maybe-configure builder configure-client-options))))

(defn make-client
  [{:keys [service-stubs workflow-client connect? connect-timeout-ms] :as opts}]
  (let [target (non-blank-string (:target opts) :target default-target)
        namespace (non-blank-string (:namespace opts) :namespace default-namespace)
        service-stubs (or service-stubs
                          (WorkflowServiceStubs/newServiceStubs (build-service-options opts)))
        workflow-client (or workflow-client
                            (WorkflowClient/newInstance service-stubs (build-client-options opts)))
        client (->TemporalClient service-stubs workflow-client target namespace opts)]
    (when connect?
      (.connect ^WorkflowServiceStubs service-stubs
                (duration (or connect-timeout-ms default-connect-timeout-ms) :connect-timeout-ms)))
    client))

(defn close!
  [^TemporalClient client]
  (when-let [^WorkflowServiceStubs service-stubs (:service-stubs client)]
    (try
      (.shutdown service-stubs)
      (catch Exception _e
        nil)))
  nil)

(defn close-now!
  [^TemporalClient client]
  (when-let [^WorkflowServiceStubs service-stubs (:service-stubs client)]
    (try
      (.shutdownNow service-stubs)
      (catch Exception _e
        nil)))
  nil)

(defn await-termination!
  [^TemporalClient client timeout-ms]
  (let [timeout-ms (positive-long timeout-ms :timeout-ms)]
    (if-let [^WorkflowServiceStubs service-stubs (:service-stubs client)]
      (.awaitTermination service-stubs timeout-ms TimeUnit/MILLISECONDS)
      true)))

(defn connect!
  ([client]
   (connect! client {:timeout-ms default-connect-timeout-ms}))
  ([^TemporalClient client {:keys [timeout-ms] :or {timeout-ms default-connect-timeout-ms}}]
   (when-let [^WorkflowServiceStubs service-stubs (:service-stubs client)]
     (.connect service-stubs (duration timeout-ms :timeout-ms)))
   client))

(defn- blocking-stub
  ^WorkflowServiceGrpc$WorkflowServiceBlockingStub
  [^TemporalClient client]
  (.blockingStub ^WorkflowServiceStubs (:service-stubs client)))

(defn system-info
  [^TemporalClient client]
  (let [response (.getSystemInfo (blocking-stub client)
                                 (GetSystemInfoRequest/getDefaultInstance))]
    {:server-version (.getServerVersion response)}))

(defn cluster-info
  [^TemporalClient client]
  (let [response (.getClusterInfo (blocking-stub client)
                                  (GetClusterInfoRequest/getDefaultInstance))]
    {:server-version (.getServerVersion response)
     :cluster-name (.getClusterName response)
     :cluster-id (.getClusterId response)
     :history-shard-count (.getHistoryShardCount response)
     :persistence-store (.getPersistenceStore response)}))

(defn health
  [^TemporalClient client]
  (try
    {:status :up
     :target (:target client)
     :namespace (:namespace client)
     :system-info (system-info client)}
    (catch Exception ex
      {:status :down
       :target (:target client)
       :namespace (:namespace client)
       :error (.getMessage ex)})))

(defn build-workflow-options
  ^WorkflowOptions
  [{:keys [task-queue workflow-id workflow-execution-timeout-ms workflow-run-timeout-ms
           workflow-task-timeout-ms memo search-attributes retry-options cron-schedule
           configure-workflow-options]}]
  (let [builder (WorkflowOptions/newBuilder)]
    (when task-queue
      (.setTaskQueue builder (->string task-queue)))
    (when workflow-id
      (.setWorkflowId builder (->string workflow-id)))
    (when-let [v (duration workflow-execution-timeout-ms :workflow-execution-timeout-ms)]
      (.setWorkflowExecutionTimeout builder v))
    (when-let [v (duration workflow-run-timeout-ms :workflow-run-timeout-ms)]
      (.setWorkflowRunTimeout builder v))
    (when-let [v (duration workflow-task-timeout-ms :workflow-task-timeout-ms)]
      (.setWorkflowTaskTimeout builder v))
    (when memo
      (.setMemo builder memo))
    (when search-attributes
      (.setSearchAttributes builder search-attributes))
    (when retry-options
      (.setRetryOptions builder retry-options))
    (when cron-schedule
      (.setCronSchedule builder (->string cron-schedule)))
    (.build (maybe-configure builder configure-workflow-options))))

(defn new-untyped-workflow-stub
  (^WorkflowStub [client workflow-type]
   (new-untyped-workflow-stub client workflow-type nil))
  (^WorkflowStub [^TemporalClient client workflow-type opts]
   (let [workflow-type (non-blank-string workflow-type :workflow-type nil)]
     (if opts
       (.newUntypedWorkflowStub ^WorkflowClient (:workflow-client client)
                                workflow-type
                                (build-workflow-options opts))
       (.newUntypedWorkflowStub ^WorkflowClient (:workflow-client client)
                                workflow-type)))))

(defn workflow-execution->map
  [^WorkflowExecution execution]
  {:workflow-id (.getWorkflowId execution)
   :run-id (.getRunId execution)})

(defn start-untyped-workflow!
  [^TemporalClient client workflow-type opts & args]
  (let [^WorkflowStub stub (new-untyped-workflow-stub client workflow-type opts)
        execution (.start stub (object-array args))]
    (assoc (workflow-execution->map execution)
           :workflow-type (->string workflow-type)
           :task-queue (:task-queue opts))))
