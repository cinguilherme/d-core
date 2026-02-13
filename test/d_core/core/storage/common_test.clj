(ns d-core.core.storage.common-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.storage.common :as common]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.helpers.logger :as h-logger]))

;; ---------------------------------------------------------------------------
;; Test infrastructure
;; ---------------------------------------------------------------------------

;; Java interface so that mock instruments respond to the .labels interop call
;; used by record-metrics! in common.clj.
(definterface ILabelable
  (labels [^"[Ljava.lang.String;" label-values]))

(defn- make-labelable
  "Returns an ILabelable whose .labels returns a map tag for assertion matching."
  [collector-name]
  (reify ILabelable
    (labels [_ label-values]
      {:collector collector-name :labels (vec label-values)})))

(defn- make-mock-metrics
  "Returns {:metrics <MetricsProtocol reification> :calls <atom>}.
   Every call to inc!/observe! is appended as a map to the calls atom."
  []
  (let [calls (atom [])]
    {:calls   calls
     :metrics (reify metrics/MetricsProtocol
                (registry [_] nil)
                (counter [_ opts]
                  (make-labelable (:name opts)))
                (gauge [_ opts]
                  (make-labelable (:name opts)))
                (histogram [_ opts]
                  (make-labelable (:name opts)))
                (inc! [_ metric]
                  (swap! calls conj {:op :inc! :metric metric :amount 1}))
                (inc! [_ metric amount]
                  (swap! calls conj {:op :inc! :metric metric :amount amount}))
                (observe! [_ histogram value]
                  (swap! calls conj {:op :observe! :metric histogram :value value})))}))

(defn- make-mock-delegate
  "Returns a StorageProtocol reification.
   `behaviors` is a map of method keyword to a canned return value.
   When the value is a Throwable it is thrown; otherwise returned directly."
  [behaviors]
  (letfn [(resolve-behavior [k]
            (let [v (get behaviors k)]
              (if (instance? Throwable v)
                (throw v)
                v)))]
    (reify storage/StorageProtocol
      (storage-get       [_ _key _opts]         (resolve-behavior :get))
      (storage-put       [_ _key _value _opts]  (resolve-behavior :put))
      (storage-delete    [_ _key _opts]         (resolve-behavior :delete))
      (storage-get-bytes [_ _key _opts]         (resolve-behavior :get-bytes))
      (storage-put-bytes [_ _key _bytes _opts]  (resolve-behavior :put-bytes))
      (storage-head      [_ _key _opts]         (resolve-behavior :head))
      (storage-list      [_ _opts]              (resolve-behavior :list)))))

(defn- build-common-storage
  "Constructs a CommonStorage for testing.
   When mock-metrics is provided, instruments are built via the mock."
  [{:keys [delegate mock-metrics logger]}]
  (let [backends    {:test delegate}
        instruments (when mock-metrics
                      {:requests-total   (metrics/counter mock-metrics
                                                          {:name   :storage_requests_total
                                                           :labels [:op :status]})
                       :request-duration (metrics/histogram mock-metrics
                                                            {:name    :storage_request_duration_seconds
                                                             :labels  [:op]})
                       :bytes-hist       (metrics/histogram mock-metrics
                                                            {:name    :storage_bytes
                                                             :labels  [:op]})})]
    (common/->CommonStorage :test backends logger mock-metrics instruments)))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- find-calls
  "Filter recorded metric calls by operation keyword and optionally collector name."
  [calls op & [collector-name]]
  (cond->> @calls
    true           (filter #(= op (:op %)))
    collector-name (filter #(= collector-name (get-in % [:metric :collector])))))

;; ---------------------------------------------------------------------------
;; 1. Delegation without metrics (metrics=nil passthrough)
;; ---------------------------------------------------------------------------

(deftest delegation-without-metrics
  (let [{:keys [logger]} (h-logger/make-test-logger)
        delegate (make-mock-delegate
                   {:get       {:ok true :value "hello"}
                    :put       {:ok true}
                    :delete    {:ok true}
                    :get-bytes {:ok true :bytes (byte-array [1 2 3])}
                    :put-bytes {:ok true}
                    :head      {:ok true :key "k" :size 3 :content-type "text/plain" :etag nil :last-modified nil}
                    :list      {:ok true :items [{:key "a"} {:key "b"}]}})
        sut (build-common-storage {:delegate delegate :logger logger})]

    (testing "storage-get delegates and returns result"
      (is (= {:ok true :value "hello"}
             (storage/storage-get sut "k" {}))))

    (testing "storage-put delegates and returns result"
      (is (= {:ok true}
             (storage/storage-put sut "k" "v" {}))))

    (testing "storage-delete delegates and returns result"
      (is (= {:ok true}
             (storage/storage-delete sut "k" {}))))

    (testing "storage-get-bytes delegates and returns result"
      (let [result (storage/storage-get-bytes sut "k" {})]
        (is (:ok result))
        (is (= [1 2 3] (vec (:bytes result))))))

    (testing "storage-put-bytes delegates and returns result"
      (is (= {:ok true}
             (storage/storage-put-bytes sut "k" (byte-array [4 5]) {}))))

    (testing "storage-head delegates and returns result"
      (is (= {:ok true :key "k" :size 3 :content-type "text/plain" :etag nil :last-modified nil}
             (storage/storage-head sut "k" {}))))

    (testing "storage-list delegates and returns result"
      (is (= {:ok true :items [{:key "a"} {:key "b"}]}
             (storage/storage-list sut {}))))))

;; ---------------------------------------------------------------------------
;; 2. Exception propagation without metrics (baseline)
;; ---------------------------------------------------------------------------

(deftest exception-propagation-without-metrics
  (let [{:keys [logger]} (h-logger/make-test-logger)
        ex (RuntimeException. "boom")
        delegate (make-mock-delegate
                   {:get ex :put ex :delete ex
                    :get-bytes ex :put-bytes ex :head ex :list ex})
        sut (build-common-storage {:delegate delegate :logger logger})]

    (testing "storage-get propagates exception"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-get sut "k" {}))))

    (testing "storage-put propagates exception"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-put sut "k" "v" {}))))

    (testing "storage-delete propagates exception"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-delete sut "k" {}))))

    (testing "storage-get-bytes propagates exception"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-get-bytes sut "k" {}))))

    (testing "storage-put-bytes propagates exception"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-put-bytes sut "k" (byte-array [1]) {}))))

    (testing "storage-head propagates exception"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-head sut "k" {}))))

    (testing "storage-list propagates exception"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-list sut {}))))))

;; ---------------------------------------------------------------------------
;; 3. Delegation with metrics — success path
;; ---------------------------------------------------------------------------

(deftest delegation-with-metrics-success
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        delegate (make-mock-delegate
                   {:get       {:ok true :value "hello"}
                    :put       {:ok true}
                    :delete    {:ok true}
                    :get-bytes {:ok true :bytes (byte-array [1 2 3])}
                    :put-bytes {:ok true}
                    :head      {:ok true :key "k" :size 3 :content-type "text/plain" :etag nil :last-modified nil}
                    :list      {:ok true :items [{:key "a"} {:key "b"}]}})
        sut (build-common-storage {:delegate delegate
                                   :mock-metrics metrics
                                   :logger logger})]

    (testing "storage-get returns result and records :ok metrics"
      (is (= {:ok true :value "hello"}
             (storage/storage-get sut "k" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["get" "ok"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-put returns result and records :ok metrics"
      (reset! calls [])
      (is (= {:ok true}
             (storage/storage-put sut "k" "v" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["put" "ok"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-delete returns result and records :ok metrics"
      (reset! calls [])
      (is (= {:ok true}
             (storage/storage-delete sut "k" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["delete" "ok"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-get-bytes returns result and records :ok metrics"
      (reset! calls [])
      (let [result (storage/storage-get-bytes sut "k" {})]
        (is (:ok result))
        (let [incs (find-calls calls :inc! :storage_requests_total)]
          (is (= 1 (count incs)))
          (is (= ["get" "ok"] (get-in (first incs) [:metric :labels]))))))

    (testing "storage-put-bytes returns result and records :ok metrics"
      (reset! calls [])
      (is (= {:ok true}
             (storage/storage-put-bytes sut "k" (byte-array [4 5]) {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["put" "ok"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-head returns result and records :ok metrics"
      (reset! calls [])
      (is (= {:ok true :key "k" :size 3 :content-type "text/plain" :etag nil :last-modified nil}
             (storage/storage-head sut "k" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["head" "ok"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-list returns result and records :ok metrics"
      (reset! calls [])
      (is (= {:ok true :items [{:key "a"} {:key "b"}]}
             (storage/storage-list sut {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["list" "ok"] (get-in (first incs) [:metric :labels])))))))

;; ---------------------------------------------------------------------------
;; 4. Exception propagation WITH metrics (catch+rethrow)
;; ---------------------------------------------------------------------------

(deftest exception-propagation-with-metrics
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        ex (RuntimeException. "boom")
        delegate (make-mock-delegate
                   {:get ex :put ex :delete ex
                    :get-bytes ex :put-bytes ex :head ex :list ex})
        sut (build-common-storage {:delegate delegate
                                   :mock-metrics metrics
                                   :logger logger})]

    (testing "storage-get: exception propagates AND error metrics recorded"
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-get sut "k" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["get" "error"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-put: exception propagates AND error metrics recorded"
      (reset! calls [])
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-put sut "k" "v" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["put" "error"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-delete: exception propagates AND error metrics recorded"
      (reset! calls [])
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-delete sut "k" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["delete" "error"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-get-bytes: exception propagates AND error metrics recorded"
      (reset! calls [])
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-get-bytes sut "k" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["get" "error"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-put-bytes: exception propagates AND error metrics recorded"
      (reset! calls [])
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-put-bytes sut "k" (byte-array [1]) {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["put" "error"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-head: exception propagates AND error metrics recorded"
      (reset! calls [])
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-head sut "k" {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["head" "error"] (get-in (first incs) [:metric :labels])))))

    (testing "storage-list: exception propagates AND error metrics recorded"
      (reset! calls [])
      (is (thrown-with-msg? RuntimeException #"boom"
            (storage/storage-list sut {})))
      (let [incs (find-calls calls :inc! :storage_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["list" "error"] (get-in (first incs) [:metric :labels])))))))

;; ---------------------------------------------------------------------------
;; 5. Byte-count histogram (success and failure)
;; ---------------------------------------------------------------------------

(deftest byte-count-metrics-on-success
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        payload (byte-array [10 20 30 40 50])
        delegate (make-mock-delegate
                   {:get-bytes {:ok true :bytes payload}
                    :put-bytes {:ok true}})
        sut (build-common-storage {:delegate delegate
                                   :mock-metrics metrics
                                   :logger logger})]

    (testing "storage-get-bytes records byte-count on success"
      (storage/storage-get-bytes sut "k" {})
      (let [obs (find-calls calls :observe! :storage_bytes)]
        (is (= 1 (count obs)))
        (is (= 5 (int (:value (first obs)))))))

    (testing "storage-put-bytes records byte-count from input arg"
      (reset! calls [])
      (storage/storage-put-bytes sut "k" payload {})
      (let [obs (find-calls calls :observe! :storage_bytes)]
        (is (= 1 (count obs)))
        (is (= 5 (int (:value (first obs)))))))

    (testing "storage-head does not record byte-count histogram"
      (reset! calls [])
      (storage/storage-head sut "k" {})
      (let [obs (find-calls calls :observe! :storage_bytes)]
        (is (empty? obs))))))

(deftest byte-count-not-recorded-on-exception
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        ex (RuntimeException. "disk-error")
        delegate (make-mock-delegate {:get-bytes ex})
        sut (build-common-storage {:delegate delegate
                                   :mock-metrics metrics
                                   :logger logger})]

    (testing "storage-get-bytes does not record byte-count on exception"
      (is (thrown? RuntimeException
            (storage/storage-get-bytes sut "k" {})))
      (let [obs (find-calls calls :observe! :storage_bytes)]
        (is (empty? obs))))))

;; ---------------------------------------------------------------------------
;; 6. :not-found status in metrics
;; ---------------------------------------------------------------------------

(deftest not-found-status-in-metrics
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        delegate (make-mock-delegate
                   {:get {:ok false :error-type :not-found}
                    :head {:ok false :error-type :not-found}})
        sut (build-common-storage {:delegate delegate
                                   :mock-metrics metrics
                                   :logger logger})]

    (testing "not-found result records :not-found status, not :error"
      (let [result (storage/storage-get sut "k" {})]
        (is (not (:ok result)))
        (let [incs (find-calls calls :inc! :storage_requests_total)]
          (is (= 1 (count incs)))
          (is (= ["get" "not-found"]
                 (get-in (first incs) [:metric :labels]))))))

    (testing "storage-head not-found result records :not-found status"
      (reset! calls [])
      (let [result (storage/storage-head sut "k" {})]
        (is (not (:ok result)))
        (let [incs (find-calls calls :inc! :storage_requests_total)]
          (is (= 1 (count incs)))
          (is (= ["head" "not-found"]
                 (get-in (first incs) [:metric :labels]))))))))
