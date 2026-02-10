(ns d-core.integration.stream-metrics-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.stream.protocol :as p]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.helpers.logger :as h-logger]
            [integrant.core :as ig]
            [d-core.core.stream.in-mem.in-mem :as in-mem]))

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

(deftest in-mem-stream-instrumentation-integration-test
  (let [{:keys [logger]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (make-mock-metrics)
        backend (ig/init-key :core-service.app.streams.in-memory/backend 
                            {:logger logger :metrics metrics})
        stream "test-stream"]
    
    (testing "append-payload! through in-mem backend triggers metrics"
      (let [payload (.getBytes "hello world")]
        (p/append-payload! backend stream payload)
        (let [incs (filter #(= :inc! (:op %)) @calls)
              obs  (filter #(= :observe! (:op %)) @calls)
              req-total (first (filter #(= :stream_requests_total (get-in % [:metric :collector])) incs))
              duration  (first (filter #(= :stream_request_duration_seconds (get-in % [:metric :collector])) obs))
              bytes     (first (filter #(= :stream_bytes (get-in % [:metric :collector])) obs))]
          (is (some? req-total) "Should have record for stream_requests_total")
          (is (some? duration) "Should have record for stream_request_duration_seconds")
          (is (some? bytes) "Should have record for stream_bytes")
          
          (when req-total
            (is (= ["append" "ok"] (get-in req-total [:metric :labels]))))
          
          (when bytes
            (is (= (double (alength payload)) (double (:value bytes))))))))))
