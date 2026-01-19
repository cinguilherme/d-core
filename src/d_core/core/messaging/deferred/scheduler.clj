(ns d-core.core.messaging.deferred.scheduler
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.messaging.deferred.protocol :as deferred]
            [d-core.core.producers.protocol :as p])
  (:import [java.util Properties Date UUID]
           [org.quartz Job JobBuilder JobDataMap Scheduler TriggerBuilder
            SimpleScheduleBuilder TriggerKey JobKey]
           [org.quartz.impl StdSchedulerFactory]))

(def ^:private job-group "d-core.deferred")
(def ^:private trigger-group "d-core.deferred")
(def ^:private ctx-producers-key "producers")
(def ^:private ctx-logger-key "logger")

(defn- log!
  [logger level event data]
  (when logger
    (logger/log logger level event data)))

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

(defn- normalize-quartz-props
  [quartz]
  (let [props (or (:properties quartz)
                  (:props quartz)
                  quartz)]
    (cond
      (nil? props) {}
      (instance? Properties props)
      (into {}
            (map (fn [[k v]] [(str k) (str v)]))
            props)
      (map? props)
      (into {}
            (map (fn [[k v]] [(str k) (str v)]))
            props)
      :else
      (throw (ex-info "Unsupported Quartz properties value" {:props props})))))

(defn- require-jdbc-jobstore!
  [quartz]
  (let [props (normalize-quartz-props quartz)
        jobstore (get props "org.quartz.jobStore.class")]
    (when (or (nil? jobstore)
              (= "org.quartz.simpl.RAMJobStore" jobstore))
      (throw (ex-info "Deferred scheduler requires JDBCJobStore configuration"
                      {:jobstore jobstore})))))

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
  [^Scheduler scheduler {:keys [producers logger]}]
  (let [ctx (.getContext scheduler)]
    (.put ctx ctx-producers-key producers)
    (.put ctx ctx-logger-key logger)))

(defn- job-key
  [id]
  (JobKey. (str id) job-group))

(defn- trigger-key
  [id]
  (TriggerKey. (str id) trigger-group))

(deftype DeferredDispatchJob []
  Job
  (execute [_ ctx]
    (let [data (.getMergedJobDataMap ctx)
          payload (.get data "payload")
          scheduler (.getScheduler ctx)
          sched-ctx (.getContext scheduler)
          producers (.get sched-ctx ctx-producers-key)
          logger (.get sched-ctx ctx-logger-key)
          producer-key (:producer payload)
          delegate (get producers producer-key)]
      (if (nil? delegate)
        (do
          (log! logger :error ::missing-producer
                {:producer producer-key :known (keys producers)})
          (throw (ex-info "Unknown producer key"
                          {:producer producer-key
                           :known (keys producers)})))
        (try
          (p/produce! delegate (:msg payload) (:options payload))
          (catch Exception e
            (log! logger :error ::deferred-produce-failed
                  {:producer producer-key
                   :error (.getMessage e)})
            (throw e)))))))

(defn- build-job
  [id payload]
  (let [data (doto (JobDataMap.)
               (.put "payload" payload))]
    (-> (JobBuilder/newJob DeferredDispatchJob)
        (.withIdentity (job-key id))
        (.usingJobData data)
        (.build))))

(defn- build-trigger
  [id deliver-at-ms]
  (let [start-at (Date. (long deliver-at-ms))
        schedule (SimpleScheduleBuilder/simpleSchedule)]
    (-> (TriggerBuilder/newTrigger)
        (.withIdentity (trigger-key id))
        (.startAt start-at)
        (.withSchedule schedule)
        (.build))))

(defrecord QuartzDeferredScheduler [scheduler logger]
  deferred/DeferredScheduler
  (schedule! [_ {:keys [deliver-at-ms] :as payload}]
    (when-not (number? deliver-at-ms)
      (throw (ex-info "Deferred schedule requires :deliver-at-ms"
                      {:payload payload})))
    (let [id (or (:id payload) (str (UUID/randomUUID)))]
      (.scheduleJob scheduler (build-job id payload) (build-trigger id deliver-at-ms))
      {:ok true
       :status :scheduled
       :backend :quartz
       :id id
       :deliver-at-ms deliver-at-ms})))

(defmethod ig/init-key :d-core.core.messaging.deferred/scheduler
  [_ {:keys [producers logger quartz start? shutdown-wait? require-jdbc?]
      :or {start? true
           shutdown-wait? true
           require-jdbc? true}}]
  (when require-jdbc?
    (require-jdbc-jobstore! quartz))
  (let [scheduler (build-scheduler quartz)
        _ (apply-scheduler-context! scheduler {:producers producers
                                               :logger logger})
        component (->QuartzDeferredScheduler scheduler logger)]
    (when start?
      (.start scheduler)
      (log! logger :info ::scheduler-started {}))
    (assoc component :shutdown-wait? shutdown-wait?)))

(defmethod ig/halt-key! :d-core.core.messaging.deferred/scheduler
  [_ {:keys [^Scheduler scheduler shutdown-wait? logger]}]
  (when scheduler
    (log! logger :info ::scheduler-stopping {})
    (.shutdown scheduler (boolean shutdown-wait?))))
