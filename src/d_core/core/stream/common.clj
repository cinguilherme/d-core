(ns d-core.core.stream.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.stream.protocol :as p]
            [d-core.core.metrics.protocol :as m]))

(defn- elapsed-seconds
  [start-nanos]
  (/ (double (- (System/nanoTime) start-nanos)) 1e9))

(defn- record-metrics!
  [metrics instruments op duration status byte-count]
  (when metrics
    (let [{:keys [requests-total request-duration bytes-hist]} instruments]
      (when requests-total
        (m/inc! metrics (.labels requests-total (into-array String [(name op) (name status)]))))
      (when request-duration
        (m/observe! metrics (.labels request-duration (into-array String [(name op)])) duration))
      (when (and bytes-hist (number? byte-count))
        (m/observe! metrics (.labels bytes-hist (into-array String [(name op)])) byte-count)))))

(defrecord CommonStream [backend logger metrics instruments]
  p/StreamBackend
  (append-payload! [_ stream payload-bytes]
    (if-not metrics
      (p/append-payload! backend stream payload-bytes)
      (let [start  (System/nanoTime)
            status (volatile! :ok)]
        (try
          (p/append-payload! backend stream payload-bytes)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :append
                             (elapsed-seconds start) @status 
                             (when payload-bytes (alength ^bytes payload-bytes))))))))

  (append-batch! [_ stream payloads-bytes]
    (if-not metrics
      (p/append-batch! backend stream payloads-bytes)
      (let [start  (System/nanoTime)
            status (volatile! :ok)
            total-bytes (reduce (fn [acc p] (+ acc (if p (alength ^bytes p) 0))) 0 payloads-bytes)]
        (try
          (p/append-batch! backend stream payloads-bytes)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :append-batch
                             (elapsed-seconds start) @status 
                             total-bytes))))))

  (read-payloads [_ stream opts]
    (if-not metrics
      (p/read-payloads backend stream opts)
      (let [start  (System/nanoTime)
            status (volatile! :ok)
            bc     (volatile! 0)]
        (try
          (let [result (p/read-payloads backend stream opts)]
            (vreset! bc (reduce (fn [acc entry] 
                                  (+ acc (if-let [p (:payload entry)] (alength ^bytes p) 0))) 
                                0 (:entries result)))
            result)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :read
                             (elapsed-seconds start) @status @bc))))))

  (trim-stream! [_ stream id]
    (if-not metrics
      (p/trim-stream! backend stream id)
      (let [start  (System/nanoTime)
            status (volatile! :ok)]
        (try
          (p/trim-stream! backend stream id)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :trim
                             (elapsed-seconds start) @status nil))))))

  (list-streams [_ pattern]
    (if-not metrics
      (p/list-streams backend pattern)
      (let [start  (System/nanoTime)
            status (volatile! :ok)]
        (try
          (p/list-streams backend pattern)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :list
                             (elapsed-seconds start) @status nil))))))

  (get-cursor [_ key]
    (if-not metrics
      (p/get-cursor backend key)
      (let [start  (System/nanoTime)
            status (volatile! :ok)]
        (try
          (p/get-cursor backend key)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :get-cursor
                             (elapsed-seconds start) @status nil))))))

  (set-cursor! [_ key cursor]
    (if-not metrics
      (p/set-cursor! backend key cursor)
      (let [start  (System/nanoTime)
            status (volatile! :ok)]
        (try
          (p/set-cursor! backend key cursor)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :set-cursor
                             (elapsed-seconds start) @status nil))))))

  (next-sequence! [_ key]
    (if-not metrics
      (p/next-sequence! backend key)
      (let [start  (System/nanoTime)
            status (volatile! :ok)]
        (try
          (p/next-sequence! backend key)
          (catch Exception e
            (vreset! status :error)
            (throw e))
          (finally
            (record-metrics! metrics instruments :next-sequence
                             (elapsed-seconds start) @status nil)))))))

(defn- build-instruments
  [metrics]
  {:requests-total   (m/counter metrics {:name   :stream_requests_total
                                         :help   "Stream requests"
                                         :labels [:op :status]})
   :request-duration (m/histogram metrics {:name    :stream_request_duration_seconds
                                           :help    "Stream request duration in seconds"
                                           :labels  [:op]
                                           :buckets [0.001 0.005 0.01 0.025 0.05
                                                     0.1 0.25 0.5 1 2 5]})
   :bytes-hist       (m/histogram metrics {:name    :stream_bytes
                                           :help    "Stream bytes transferred"
                                           :labels  [:op]
                                           :buckets [1024 4096 16383 65535 262143
                                                     1048575 5242879 10485759]})})

(defn wrap-backend
  [backend logger metrics]
  (let [instruments (when metrics (build-instruments metrics))]
    (->CommonStream backend logger metrics instruments)))

(defmethod ig/init-key :d-core.core.stream/common
  [_ {:keys [backend logger metrics]}]
  (logger/log logger :info ::initializing-common-stream {:metrics? (some? metrics)})
  (wrap-backend backend logger metrics))
