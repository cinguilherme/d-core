(ns d-core.core.metrics.wrappers
  (:require [d-core.core.metrics.protocol :as metrics]))

(defn label-value
  [value]
  (cond
    (nil? value) "unknown"
    (keyword? value) (if-let [ns (namespace value)]
                       (str ns "/" (name value))
                       (name value))
    :else (str value)))

(defn labels->array
  [& values]
  (into-array String (map label-value values)))

(defn duration-seconds
  [start-nanos]
  (/ (double (- (System/nanoTime) start-nanos)) 1000000000.0))

(defn apply-labels
  [metric labels]
  (if (ifn? metric)
    (metric labels)
    (.labels metric labels)))

(defn redis-instruments
  [metrics-api]
  {:requests-total (metrics/counter metrics-api {:name :redis_requests_total
                                                 :help "Redis requests"
                                                 :labels [:op :status]})
   :request-duration (metrics/histogram metrics-api {:name :redis_request_duration_seconds
                                                     :help "Redis request duration in seconds"
                                                     :labels [:op]
                                                     :buckets [0.001 0.005 0.01 0.025 0.05
                                                               0.1 0.25 0.5 1 2 5]})})

(defn record-redis!
  [metrics-api instruments op duration-seconds status]
  (when (and metrics-api instruments)
    (let [{:keys [requests-total request-duration]} instruments]
      (when requests-total
        (metrics/inc! metrics-api
                      (apply-labels requests-total (labels->array op status))))
      (when request-duration
        (metrics/observe! metrics-api
                          (apply-labels request-duration (labels->array op))
                          duration-seconds)))))

(defn with-redis
  [metrics-api instruments op f]
  (let [start (System/nanoTime)]
    (try
      (let [result (f)]
        (record-redis! metrics-api instruments op (duration-seconds start) :ok)
        result)
      (catch Throwable t
        (record-redis! metrics-api instruments op (duration-seconds start) :error)
        (throw t)))))
