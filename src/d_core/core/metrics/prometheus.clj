(ns d-core.core.metrics.prometheus
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [d-core.core.metrics.protocol :as p])
  (:import (io.prometheus.client CollectorRegistry Counter Gauge Histogram)
           (io.prometheus.client.exporter HTTPServer)
           (io.prometheus.client.hotspot DefaultExports)
           (java.net InetSocketAddress)))

(defn- normalize-name [x]
  (-> (name x)
      (str/replace "-" "_")))

(defn- ->label-names [labels]
  (into-array String (map name (or labels []))))

(defn- ->buckets [buckets]
  (when (seq buckets)
    (double-array (map double buckets))))

(defn- build-counter [{:keys [name help labels registry]}]
  (-> (Counter/build)
      (.name (normalize-name name))
      (.help (or help (str (normalize-name name) " counter")))
      (.labelNames (->label-names labels))
      (.register ^CollectorRegistry registry)))

(defn- build-gauge [{:keys [name help labels registry]}]
  (-> (Gauge/build)
      (.name (normalize-name name))
      (.help (or help (str (normalize-name name) " gauge")))
      (.labelNames (->label-names labels))
      (.register ^CollectorRegistry registry)))

(defn- build-histogram [{:keys [name help labels buckets registry]}]
  (let [builder (-> (Histogram/build)
                    (.name (normalize-name name))
                    (.help (or help (str (normalize-name name) " histogram")))
                    (.labelNames (->label-names labels)))]
    (when-let [b (->buckets buckets)]
      (.buckets builder b))
    (.register builder ^CollectorRegistry registry)))

(defrecord PrometheusMetrics [^CollectorRegistry registry]
  p/MetricsProtocol
  (registry [_] registry)
  (counter [_ opts]
    (build-counter (assoc opts :registry registry)))
  (gauge [_ opts]
    (build-gauge (assoc opts :registry registry)))
  (histogram [_ opts]
    (build-histogram (assoc opts :registry registry)))
  (inc! [_ metric]
    (.inc metric))
  (inc! [_ metric amount]
    (.inc metric (double amount)))
  (observe! [_ histogram value]
    (.observe histogram (double value))))

(defonce ^:private jvm-metrics-registered? (atom false))

(defn- ensure-jvm-metrics!
  []
  (when (compare-and-set! jvm-metrics-registered? false true)
    (DefaultExports/initialize)))

(defmethod ig/init-key :d-core.core.metrics.prometheus/registry
  [_ {:keys [jvm-metrics?]
      :or {jvm-metrics? true}}]
  (if jvm-metrics?
    (do
      (ensure-jvm-metrics!)
      CollectorRegistry/defaultRegistry)
    (CollectorRegistry.)))

(defmethod ig/init-key :d-core.core.metrics.prometheus/metrics
  [_ {:keys [registry]}]
  (->PrometheusMetrics registry))

(defmethod ig/init-key :d-core.core.metrics.prometheus/server
  [_ {:keys [port host registry daemon?]
      :or {host "0.0.0.0" daemon? true}}]
  {:server (HTTPServer. (InetSocketAddress. ^String host (int port))
                        ^CollectorRegistry registry
                        (boolean daemon?))
   :port port
   :host host
   :registry registry})

(defmethod ig/halt-key! :d-core.core.metrics.prometheus/server
  [_ {:keys [^HTTPServer server]}]
  (when server
    (.stop server)))
