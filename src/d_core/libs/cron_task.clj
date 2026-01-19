(ns d-core.libs.cron-task
  (:require [integrant.core :as ig]
            [duct.logger :as logger])
  (:import [java.util Properties TimeZone]
           [org.quartz CronExpression CronScheduleBuilder Job JobBuilder JobDataMap
            JobKey Scheduler TriggerBuilder TriggerKey]
           [org.quartz.impl StdSchedulerFactory]
           [org.quartz.impl.matchers GroupMatcher]))

(def ^:private job-group "d-core.cron-task")
(def ^:private trigger-group "d-core.cron-task")
(def ^:private ctx-handlers-key "handlers")
(def ^:private ctx-deps-key "deps")
(def ^:private ctx-logger-key "logger")

(defn- log!
  [logger level event data]
  (when logger
    (logger/log logger level event data)))

(defn- id->string
  [value]
  (cond
    (nil? value) nil
    (keyword? value) (subs (str value) 1)
    :else (str value)))

(defn- string->keyword
  [value]
  (when value
    (let [s (str value)
          s (if (and (seq s) (= (first s) \:)) (subs s 1) s)]
      (keyword s))))

(defn- parse-id
  [value]
  (cond
    (nil? value) nil
    (keyword? value) value
    (string? value) (string->keyword value)
    (symbol? value) (keyword (str value))
    :else (string->keyword value)))

(defn- resolve-handler
  [handlers handler-id]
  (when handlers
    (or (get handlers handler-id)
        (when (string? handler-id)
          (get handlers (string->keyword handler-id)))
        (when (keyword? handler-id)
          (get handlers (str handler-id))))))

(defn- normalize-task
  [task-id task]
  (when-not (map? task)
    (throw (ex-info "Cron task must be a map" {:task task :id task-id})))
  (let [id (or (:id task) task-id)]
    (when-not id
      (throw (ex-info "Cron task is missing :id" {:task task})))
    (cond-> (assoc task :id id)
      (not (contains? task :enabled)) (assoc :enabled true))))

(defn- normalize-tasks
  [tasks]
  (cond
    (nil? tasks) []
    (map? tasks)
    (mapv (fn [[task-id task]] (normalize-task task-id task)) tasks)
    (sequential? tasks)
    (mapv (fn [task] (normalize-task (:id task) task)) tasks)
    :else
    (throw (ex-info "Unsupported tasks shape" {:tasks tasks}))))

(def ^:private misfire-handlings
  #{:fire-now :do-nothing :ignore-misfires})

(defn- validate-task!
  [task handlers]
  (let [{:keys [id cron handler timezone misfire]} task]
    (when-not id
      (throw (ex-info "Cron task is missing :id" {:task task})))
    (when-not (and (string? cron) (CronExpression/isValidExpression cron))
      (throw (ex-info "Invalid cron expression" {:task-id id :cron cron})))
    (when-not handler
      (throw (ex-info "Cron task is missing :handler" {:task-id id})))
    (when (and handlers (nil? (resolve-handler handlers handler)))
      (throw (ex-info "Cron task handler is not registered"
                      {:task-id id :handler handler})))
    (when (and (some? misfire) (not (misfire-handlings misfire)))
      (throw (ex-info "Unsupported misfire handling"
                      {:task-id id :misfire misfire :supported misfire-handlings})))
    (when (and (some? timezone)
               (not (or (string? timezone)
                        (keyword? timezone)
                        (instance? TimeZone timezone))))
      (throw (ex-info "Unsupported timezone value" {:task-id id :timezone timezone}))))
  task)

(defn- coerce-timezone
  [timezone]
  (cond
    (nil? timezone) nil
    (instance? TimeZone timezone) timezone
    (keyword? timezone) (TimeZone/getTimeZone (name timezone))
    (string? timezone) (TimeZone/getTimeZone timezone)
    :else (throw (ex-info "Unsupported timezone value" {:timezone timezone}))))

(defn- job-key
  [task-id]
  (JobKey. (id->string task-id) job-group))

(defn- trigger-key
  [task-id]
  (TriggerKey. (id->string task-id) trigger-group))

(defn- task-job-data
  [{:keys [id handler payload]}]
  (doto (JobDataMap.)
    (.put "task-id" (id->string id))
    (.put "handler-id" (id->string handler))
    (.put "payload" payload)))

(defn- cron-schedule-builder
  [{:keys [cron timezone misfire]}]
  (let [builder (CronScheduleBuilder/cronSchedule cron)
        builder (if timezone
                  (.inTimeZone builder (coerce-timezone timezone))
                  builder)]
    (case misfire
      :fire-now (.withMisfireHandlingInstructionFireAndProceed builder)
      :do-nothing (.withMisfireHandlingInstructionDoNothing builder)
      :ignore-misfires (.withMisfireHandlingInstructionIgnoreMisfires builder)
      builder)))

(defn- props->properties
  [props]
  (cond
    (nil? props) nil
    (instance? Properties props) props
    (map? props)
    (let [p (Properties.)]
      (doseq [[k v] props]
        (when (some? v)
          (.setProperty p (str k) (str v))))
      p)
    :else
    (throw (ex-info "Unsupported Quartz properties value" {:props props}))))

(defn- build-scheduler
  [quartz]
  (let [props (props->properties (or (:properties quartz)
                                     (:props quartz)
                                     quartz))
        factory (StdSchedulerFactory.)]
    (when props
      (.initialize factory props))
    (.getScheduler factory)))

(defn- apply-scheduler-context!
  [^Scheduler scheduler {:keys [handlers deps logger context]}]
  (let [ctx (.getContext scheduler)
        entries (merge {ctx-handlers-key handlers
                        ctx-deps-key deps
                        ctx-logger-key logger}
                       (or context {}))]
    (doseq [[k v] entries]
      (.put ctx (str k) v))))

(defn- job-input
  [ctx task-id handler-id payload deps]
  {:task-id task-id
   :handler handler-id
   :payload payload
   :deps deps
   :context ctx
   :fire-time (.getFireTime ctx)
   :scheduled-fire-time (.getScheduledFireTime ctx)
   :next-fire-time (.getNextFireTime ctx)
   :previous-fire-time (.getPreviousFireTime ctx)})

(deftype CronDispatchJob []
  Job
  (execute [_ ctx]
    (let [data (.getMergedJobDataMap ctx)
          task-id-raw (.get data "task-id")
          handler-id-raw (.get data "handler-id")
          task-id (parse-id task-id-raw)
          handler-id (parse-id handler-id-raw)
          payload (.get data "payload")
          scheduler (.getScheduler ctx)
          sched-ctx (.getContext scheduler)
          handlers (.get sched-ctx ctx-handlers-key)
          deps (.get sched-ctx ctx-deps-key)
          logger (.get sched-ctx ctx-logger-key)
          handler (resolve-handler handlers handler-id-raw)]
      (if (nil? handler)
        (log! logger :error ::missing-handler
              {:task-id task-id :handler handler-id})
        (try
          (handler (job-input ctx task-id handler-id payload deps))
          (catch Exception e
            (log! logger :error ::handler-failed
                  {:task-id task-id
                   :handler handler-id
                   :error (.getMessage e)})
            (throw e)))))))

(defn- build-job
  [task]
  (let [builder (cond-> (JobBuilder/newJob CronDispatchJob)
                  (:description task) (.withDescription (str (:description task))))]
    (-> builder
        (.withIdentity (job-key (:id task)))
        (.usingJobData (task-job-data task))
        (.build))))

(defn- build-trigger
  [task]
  (let [builder (cond-> (TriggerBuilder/newTrigger)
                  (:description task) (.withDescription (str (:description task))))]
    (-> builder
        (.withIdentity (trigger-key (:id task)))
        (.forJob (job-key (:id task)))
        (.withSchedule (cron-schedule-builder task))
        (.build))))

(defn- upsert-task-internal!
  [^Scheduler scheduler task]
  (.deleteJob scheduler (job-key (:id task)))
  (when (:enabled task)
    (.scheduleJob scheduler (build-job task) (build-trigger task)))
  task)

(defn- delete-task-internal!
  [^Scheduler scheduler task-id]
  (.deleteJob scheduler (job-key task-id)))

(defn- existing-job-names
  [^Scheduler scheduler]
  (map #(.getName ^JobKey %)
       (.getJobKeys scheduler (GroupMatcher/jobGroupEquals job-group))))

(defn upsert-task!
  "Creates or replaces a cron task.

  Accepts either a task map (with :id) or a task-id + task map."
  ([cron task]
   (upsert-task! cron (:id task) task))
  ([{:keys [scheduler handlers logger]} task-id task]
   (let [task (normalize-task task-id task)]
     (validate-task! task handlers)
     (upsert-task-internal! scheduler task)
     (log! logger :report ::task-upserted {:task-id (:id task)})
     task)))

(defn delete-task!
  [{:keys [scheduler logger]} task-id]
  (delete-task-internal! scheduler task-id)
  (log! logger :report ::task-deleted {:task-id task-id})
  true)

(defn pause-task!
  [{:keys [scheduler logger]} task-id]
  (.pauseJob scheduler (job-key task-id))
  (log! logger :report ::task-paused {:task-id task-id})
  true)

(defn resume-task!
  [{:keys [scheduler logger]} task-id]
  (.resumeJob scheduler (job-key task-id))
  (log! logger :report ::task-resumed {:task-id task-id})
  true)

(defn list-task-ids
  [{:keys [scheduler]}]
  (mapv string->keyword (existing-job-names scheduler)))

(defn sync-tasks!
  ([cron] (sync-tasks! cron (:tasks cron) nil))
  ([cron tasks] (sync-tasks! cron tasks nil))
  ([{:keys [scheduler handlers sync-mode logger] :as cron} tasks opts]
   (let [sync-mode (or (:sync-mode opts) sync-mode :replace)
         tasks (normalize-tasks tasks)
         tasks-by-id (into {} (map (fn [task] [(id->string (:id task)) task]) tasks))
         configured-ids (set (keys tasks-by-id))]
     (doseq [task tasks]
       (validate-task! task handlers))
     (when (= sync-mode :replace)
       (doseq [job-name (existing-job-names scheduler)]
         (when-not (contains? configured-ids job-name)
           (.deleteJob scheduler (job-key job-name))
           (log! logger :report ::task-pruned {:task-id job-name}))))
     (doseq [[_ task] tasks-by-id]
       (if (:enabled task)
         (upsert-task-internal! scheduler task)
         (delete-task-internal! scheduler (:id task))))
     (assoc cron :tasks tasks))))

(defmethod ig/init-key :d-core.libs.cron-task/scheduler
  [_ {:keys [handlers tasks deps logger context sync-mode start? shutdown-wait? quartz]
      :or {sync-mode :replace
           start? true
           shutdown-wait? true}}]
  (let [scheduler (build-scheduler quartz)
        _ (apply-scheduler-context! scheduler {:handlers handlers
                                               :deps deps
                                               :logger logger
                                               :context context})
        base {:scheduler scheduler
              :handlers handlers
              :deps deps
              :logger logger
              :sync-mode sync-mode
              :shutdown-wait? shutdown-wait?}
        component (sync-tasks! base tasks {:sync-mode sync-mode})]
    (when start?
      (.start scheduler)
      (log! logger :report ::scheduler-started {:tasks (count (:tasks component))}))
    component))

(defmethod ig/halt-key! :d-core.libs.cron-task/scheduler
  [_ {:keys [^Scheduler scheduler shutdown-wait? logger]}]
  (when scheduler
    (log! logger :report ::scheduler-stopping {})
    (.shutdown scheduler (boolean shutdown-wait?))))
