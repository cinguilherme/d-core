(ns d-core.core.leader-election.observability
  (:require [clojure.string :as str]
            [d-core.core.leader-election.common :as common]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.core.metrics.wrappers :as metrics-wrappers]
            [duct.logger :as logger])
  (:import (java.util Collections WeakHashMap)))

(def ^:private duration-buckets
  [0.001 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2 5])

(def ^:private status->level
  {:acquired :info
   :released :info
   :lost :warn
   :busy :debug
   :renewed :debug
   :not-owner :debug})

(def ^:private safe-error-keys
  [:status :field :kind])

(defonce ^:private instruments-cache
  (Collections/synchronizedMap (WeakHashMap.)))

(defn validate-metrics!
  [metrics]
  (when (and metrics (not (satisfies? metrics/MetricsProtocol metrics)))
    (throw (ex-info "Invalid :metrics dependency for leader election. Expected a MetricsProtocol implementation."
                    {:component :d-core.core.leader-election
                     :dependency :metrics
                     :expected "d-core.core.metrics.protocol/MetricsProtocol"
                     :actual-type (.getName (class metrics))
                     :hint "Wire :metrics directly to a MetricsProtocol implementation, e.g. :d-core.core.metrics.prometheus/metrics."})))
  metrics)

(defn build-instruments
  [metrics]
  {:requests-total (metrics/counter metrics {:name :leader_election_requests_total
                                             :help "Leader election requests"
                                             :labels [:backend :op :status]})
   :request-duration (metrics/histogram metrics {:name :leader_election_request_duration_seconds
                                                 :help "Leader election request duration in seconds"
                                                 :labels [:backend :op]
                                                 :buckets duration-buckets})
   :errors-total (metrics/counter metrics {:name :leader_election_errors_total
                                           :help "Leader election errors"
                                           :labels [:backend :op :type]})})

(defn- instrument-cache-key
  [metrics-api]
  (or (metrics/registry metrics-api) metrics-api))

(defn- cached-instruments
  [metrics-api]
  (let [cache-key (instrument-cache-key metrics-api)]
    (or (.get instruments-cache cache-key)
        (locking instruments-cache
          (or (.get instruments-cache cache-key)
              (let [instruments (build-instruments metrics-api)]
                (.put instruments-cache cache-key instruments)
                instruments))))))

(defn make-context
  [logger metrics]
  (let [validated-metrics (validate-metrics! metrics)]
    {:logger logger
     :metrics validated-metrics
     :instruments (when validated-metrics
                    (cached-instruments validated-metrics))}))

(defn- safe-election-id
  [election-id]
  (let [value (some-> election-id common/normalize-key-part)]
    (when-not (str/blank? value)
      value)))

(defn- error-type
  [error]
  (or (:type (ex-data error)) :exception))

(defn- log-result!
  [logger backend op result]
  (when-let [level (get status->level (:status result))]
    (logger/log logger level ::operation-result
                (cond-> {:backend backend
                         :op op
                         :status (:status result)
                         :election-id (:election-id result)}
                  (:owner-id result) (assoc :owner-id (:owner-id result))
                  (:fencing result) (assoc :fencing (:fencing result))
                  (:remaining-ttl-ms result) (assoc :remaining-ttl-ms (:remaining-ttl-ms result))))))

(defn- log-error!
  [logger backend op election-id error]
  (let [safe-error-data (select-keys (or (ex-data error) {}) safe-error-keys)]
    (logger/log logger :error ::operation-failed
                (cond-> {:backend backend
                         :op op
                         :type (error-type error)
                         :error (.getMessage error)}
                  (safe-election-id election-id) (assoc :election-id (safe-election-id election-id))
                  (:status safe-error-data) (assoc :status (:status safe-error-data))
                  (:field safe-error-data) (assoc :field (:field safe-error-data))
                  (:kind safe-error-data) (assoc :kind (:kind safe-error-data))))))

(defn- record-duration!
  [metrics-api instruments backend op duration]
  (when-let [request-duration (:request-duration instruments)]
    (metrics/observe! metrics-api
                      (metrics-wrappers/apply-labels request-duration
                                                     (metrics-wrappers/labels->array backend op))
                      duration)))

(defn- record-result!
  [metrics-api instruments backend op result duration]
  (when (and metrics-api instruments)
    (when-let [requests-total (:requests-total instruments)]
      (metrics/inc! metrics-api
                    (metrics-wrappers/apply-labels requests-total
                                                   (metrics-wrappers/labels->array backend op (:status result)))))
    (record-duration! metrics-api instruments backend op duration)))

(defn- record-error!
  [metrics-api instruments backend op error duration]
  (when (and metrics-api instruments)
    (when-let [errors-total (:errors-total instruments)]
      (metrics/inc! metrics-api
                    (metrics-wrappers/apply-labels errors-total
                                                   (metrics-wrappers/labels->array backend op (error-type error)))))
    (record-duration! metrics-api instruments backend op duration)))

(defn- safe-observe!
  [f]
  (try
    (f)
    (catch Throwable _
      nil)))

(defn observe-operation
  [observability backend op election-id thunk]
  (let [start (System/nanoTime)
        {:keys [logger metrics instruments]} observability]
    (try
      (let [result (thunk)
            duration (metrics-wrappers/duration-seconds start)]
        (safe-observe! #(record-result! metrics instruments backend op result duration))
        (when logger
          (safe-observe! #(log-result! logger backend op result)))
        result)
      (catch Throwable error
        (let [duration (metrics-wrappers/duration-seconds start)]
          (safe-observe! #(record-error! metrics instruments backend op error duration))
          (when logger
            (safe-observe! #(log-error! logger backend op election-id error)))
          (throw error))))))
