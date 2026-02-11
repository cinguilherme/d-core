(ns d-core.core.stream.common-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.stream.common :as common]
            [d-core.core.stream.protocol :as protocol]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.helpers.logger :as h-logger]))

(definterface ILabelable
  (labels [^"[Ljava.lang.String;" label-values]))

(defn- make-labelable
  [collector-name]
  (reify ILabelable
    (labels [_ label-values]
      {:collector collector-name :labels (vec label-values)})))

(defn- make-mock-metrics
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
  [behaviors]
  (letfn [(resolve-behavior [k]
            (let [v (get behaviors k)]
              (if (instance? Throwable v)
                (throw v)
                v)))]
    (reify protocol/StreamBackend
      (append-payload! [_ _ _]            (resolve-behavior :append))
      (append-batch!   [_ _ _]            (resolve-behavior :append-batch))
      (read-payloads   [_ _ _]            (resolve-behavior :read))
      (trim-stream!    [_ _ _]            (resolve-behavior :trim))
      (list-streams    [_ _]              (resolve-behavior :list))
      (get-cursor      [_ _]              (resolve-behavior :get-cursor))
      (set-cursor!     [_ _ _]            (resolve-behavior :set-cursor))
      (next-sequence!  [_ _]              (resolve-behavior :next-sequence)))))

(defn- build-common-stream
  [{:keys [delegate mock-metrics logger]}]
  (let [instruments (when mock-metrics
                      {:requests-total   (metrics/counter mock-metrics
                                                          {:name   :stream_requests_total
                                                           :labels [:op :status]})
                       :request-duration (metrics/histogram mock-metrics
                                                            {:name    :stream_request_duration_seconds
                                                             :labels  [:op]})
                       :bytes-hist       (metrics/histogram mock-metrics
                                                            {:name    :stream_bytes
                                                             :labels  [:op]})})]
    (common/->CommonStream delegate logger mock-metrics instruments)))

(defn- find-calls
  [calls op & [collector-name]]
  (cond->> @calls
    true           (filter #(= op (:op %)))
    collector-name (filter #(= collector-name (get-in % [:metric :collector])))))

(deftest delegation-without-metrics
  (let [{:keys [logger]} (h-logger/make-test-logger)
        delegate (make-mock-delegate
                   {:append "id-1"
                    :read   {:entries [] :next-cursor nil}})
        stream   (build-common-stream {:delegate delegate :logger logger})]
    
    (testing "append-payload! returns result from delegate"
      (is (= "id-1" (protocol/append-payload! stream "s1" (.getBytes "data")))))
    
    (testing "read-payloads returns result from delegate"
      (is (= {:entries [] :next-cursor nil} (protocol/read-payloads stream "s1" {}))))))

(deftest instrumentation-success
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        payload (.getBytes "hello")
        delegate (make-mock-delegate
                   {:append "id-1"
                    :read   {:entries [{:id "1" :payload payload}] :next-cursor "2"}})
        stream   (build-common-stream {:delegate delegate 
                                       :mock-metrics metrics 
                                       :logger logger})]
    
    (testing "append-payload! records metrics"
      (is (= "id-1" (protocol/append-payload! stream "s1" payload)))
      
      (let [incs (find-calls calls :inc! :stream_requests_total)
            obs  (find-calls calls :observe! :stream_request_duration_seconds)
            bytes (find-calls calls :observe! :stream_bytes)]
        (is (= 1 (count incs)))
        (is (= ["append" "ok"] (get-in (first incs) [:metric :labels])))
        (is (= 1 (count obs)))
        (is (= ["append"] (get-in (first obs) [:metric :labels])))
        (is (= 1 (count bytes)))
        (is (= ["append"] (get-in (first bytes) [:metric :labels])))
        (is (= (double (alength payload)) (double (:value (first bytes)))))))

    (testing "read-payloads records metrics with byte count"
      (reset! calls [])
      (is (= {:entries [{:id "1" :payload payload}] :next-cursor "2"} 
             (protocol/read-payloads stream "s1" {})))
      
      (let [incs (find-calls calls :inc! :stream_requests_total)
            bytes (find-calls calls :observe! :stream_bytes)]
        (is (= 1 (count incs)))
        (is (= ["read" "ok"] (get-in (first incs) [:metric :labels])))
        (is (= 1 (count bytes)))
        (is (= ["read"] (get-in (first bytes) [:metric :labels])))
        (is (= (double (alength payload)) (double (:value (first bytes)))))))))

(deftest instrumentation-failure
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        delegate (make-mock-delegate
                   {:append (RuntimeException. "fail")})
        stream   (build-common-stream {:delegate delegate 
                                       :mock-metrics metrics 
                                       :logger logger})]
    
    (testing "append-payload! records error metrics"
      (is (thrown-with-msg? RuntimeException #"fail" 
                            (protocol/append-payload! stream "s1" (.getBytes "data"))))
      
      (let [incs (find-calls calls :inc! :stream_requests_total)]
        (is (= 1 (count incs)))
        (is (= ["append" "error"] (get-in (first incs) [:metric :labels])))))))

(deftest invalid-metrics-dependency-fails-fast
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics]} (make-mock-metrics)
        delegate          (make-mock-delegate {})]
    (testing "wrap-backend rejects wrapper maps that do not implement MetricsProtocol"
      (let [ex (try
                 (common/wrap-backend delegate logger {:metrics metrics})
                 nil
                 (catch clojure.lang.ExceptionInfo e
                   e))]
        (is (some? ex))
        (is (re-find #"Invalid :metrics dependency" (ex-message ex)))
        (is (= :d-core.core.stream/common (:component (ex-data ex))))
        (is (= :metrics (:dependency (ex-data ex))))
        (is (= "d-core.core.metrics.protocol/MetricsProtocol" (:expected (ex-data ex))))
        (is (= "clojure.lang.PersistentArrayMap" (:actual-type (ex-data ex))))))))
