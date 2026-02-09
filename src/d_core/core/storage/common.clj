(ns d-core.core.storage.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.storage.protocol :as p]
            [d-core.core.metrics.protocol :as m]))

(def ^:private byte-array-class (class (byte-array 0)))

(defn- result-status
  [result]
  (cond
    (:ok result)                        :ok
    (= :not-found (:error-type result)) :not-found
    :else                               :error))

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

(defn- byte-count
  [value]
  (when (instance? byte-array-class value)
    (alength ^bytes value)))

(defrecord CommonStorage [default-storage-key backends logger metrics instruments]
  p/StorageProtocol
  (storage-get [_ key opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-get {:key key :storage storage-key})
      (if-not metrics
        (p/storage-get delegate key opts)
        (let [start  (System/nanoTime)
              result (try
                       (p/storage-get delegate key opts)
                       (catch Exception e
                         {:ok false :key key :error (.getMessage e)}))
              status (result-status result)
              dur    (elapsed-seconds start)]
          (record-metrics! metrics instruments :get dur status nil)
          result))))
  (storage-put [_ key value opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-put {:key key :storage storage-key})
      (if-not metrics
        (p/storage-put delegate key value opts)
        (let [start  (System/nanoTime)
              result (try
                       (p/storage-put delegate key value opts)
                       (catch Exception e
                         {:ok false :key key :error (.getMessage e)}))
              status (result-status result)
              dur    (elapsed-seconds start)]
          (record-metrics! metrics instruments :put dur status nil)
          result))))
  (storage-delete [_ key opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-delete {:key key :storage storage-key})
      (if-not metrics
        (p/storage-delete delegate key opts)
        (let [start  (System/nanoTime)
              result (try
                       (p/storage-delete delegate key opts)
                       (catch Exception e
                         {:ok false :key key :error (.getMessage e)}))
              status (result-status result)
              dur    (elapsed-seconds start)]
          (record-metrics! metrics instruments :delete dur status nil)
          result))))
  (storage-get-bytes [_ key opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-get-bytes {:key key :storage storage-key})
      (if-not metrics
        (p/storage-get-bytes delegate key opts)
        (let [start  (System/nanoTime)
              result (try
                       (p/storage-get-bytes delegate key opts)
                       (catch Exception e
                         {:ok false :key key :error (.getMessage e)}))
              status (result-status result)
              dur    (elapsed-seconds start)]
          (record-metrics! metrics instruments :get dur status
                           (when (:ok result) (byte-count (:bytes result))))
          result))))
  (storage-put-bytes [_ key bytes opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-put-bytes {:key key :storage storage-key})
      (if-not metrics
        (p/storage-put-bytes delegate key bytes opts)
        (let [start  (System/nanoTime)
              result (try
                       (p/storage-put-bytes delegate key bytes opts)
                       (catch Exception e
                         {:ok false :key key :error (.getMessage e)}))
              status (result-status result)
              dur    (elapsed-seconds start)]
          (record-metrics! metrics instruments :put dur status (byte-count bytes))
          result))))
  (storage-list [_ opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-list {:storage storage-key})
      (if-not metrics
        (p/storage-list delegate opts)
        (let [start  (System/nanoTime)
              result (try
                       (p/storage-list delegate opts)
                       (catch Exception e
                         {:ok false :error (.getMessage e)}))
              status (result-status result)
              dur    (elapsed-seconds start)]
          (record-metrics! metrics instruments :list dur status nil)
          result)))))

(defn- build-instruments
  [metrics]
  {:requests-total   (m/counter metrics {:name   :storage_requests_total
                                         :help   "Storage requests"
                                         :labels [:op :status]})
   :request-duration (m/histogram metrics {:name    :storage_request_duration_seconds
                                           :help    "Storage request duration in seconds"
                                           :labels  [:op]
                                           :buckets [0.001 0.005 0.01 0.025 0.05
                                                     0.1 0.25 0.5 1 2 5]})
   :bytes-hist       (m/histogram metrics {:name    :storage_bytes
                                           :help    "Storage bytes transferred"
                                           :labels  [:op]
                                           :buckets [1024 4096 16384 65536 262144
                                                     1048576 5242880 10485760]})})

(defmethod ig/init-key :d-core.core.storage/common
  [_ {:keys [default-storage backends logger metrics]
      :or {default-storage :local-disk}}]
  (logger/log logger :info ::initializing-common-storage {:default-storage default-storage
                                                          :metrics? (some? metrics)})
  (let [instruments (when metrics (build-instruments metrics))]
    (->CommonStorage default-storage backends logger metrics instruments)))
