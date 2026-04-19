(ns d-core.core.metrics.prometheus-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.observability :as obs]
            [integrant.core :as ig]
            [d-core.core.metrics.protocol :as p]
            [d-core.core.metrics.prometheus :as prom])
  (:import (io.prometheus.client CollectorRegistry)
           (io.prometheus.client.exporter HTTPServer)))

(defn- registry-names [^CollectorRegistry reg]
  (->> (.metricFamilySamples reg)
       enumeration-seq
       (map (fn [^io.prometheus.client.Collector$MetricFamilySamples mfs]
              (.name mfs)))))

(deftest prometheus-registry-jvm-metrics
  (testing "JVM metrics are registered when enabled"
    (let [reg (ig/init-key :d-core.core.metrics.prometheus/registry {:jvm-metrics? true})
          names (registry-names reg)]
      (is (some #(str/starts-with? % "jvm_") names)))))

(deftest prometheus-registry-no-jvm-metrics
  (testing "JVM metrics are not registered when disabled"
    (let [reg (ig/init-key :d-core.core.metrics.prometheus/registry {:jvm-metrics? false})
          names (registry-names reg)]
      (is (not-any? #(str/starts-with? % "jvm_") names)))))

(deftest prometheus-metrics-counter-gauge-histogram
  (testing "Counters, gauges, and histograms register and update values"
    (let [reg (ig/init-key :d-core.core.metrics.prometheus/registry {:jvm-metrics? false})
          metrics (ig/init-key :d-core.core.metrics.prometheus/metrics {:registry reg})
          counter (p/counter metrics {:name :request-count})
          gauge (p/gauge metrics {:name :queue-depth})
          hist (p/histogram metrics {:name :latency-ms})]
      (p/inc! metrics counter)
      (p/inc! metrics counter 2)
      (p/inc! metrics gauge 5)
      (p/observe! metrics hist 3.5)
      (is (= 3.0 (.getSampleValue reg "request_count_total"
                                  (into-array String [])
                                  (into-array String []))))
      (is (= 5.0 (.getSampleValue reg "queue_depth"
                                  (into-array String [])
                                  (into-array String []))))
      (is (= 1.0 (.getSampleValue reg "latency_ms_count"
                                  (into-array String [])
                                  (into-array String []))))
      (is (= 3.5 (.getSampleValue reg "latency_ms_sum"
                                  (into-array String [])
                                  (into-array String [])))))))

(deftest prometheus-http-server-lifecycle
  (testing "HTTP server starts and stops with a registry"
    (let [reg (ig/init-key :d-core.core.metrics.prometheus/registry {:jvm-metrics? false})
          server (ig/init-key :d-core.core.metrics.prometheus/server
                              {:port 0 :registry reg})]
      (is (instance? HTTPServer (:server server)))
      (is (= "0.0.0.0" (:host server)))
      (ig/halt-key! :d-core.core.metrics.prometheus/server server))))

(deftest leader-election-observability-reuses-collectors-per-registry
  (testing "make-context reuses leader-election instruments for the same metrics registry"
    (let [reg (ig/init-key :d-core.core.metrics.prometheus/registry {:jvm-metrics? false})
          metrics (ig/init-key :d-core.core.metrics.prometheus/metrics {:registry reg})
          ctx-1 (obs/make-context nil metrics)
          ctx-2 (obs/make-context nil metrics)]
      (is (identical? (:instruments ctx-1) (:instruments ctx-2)))
      (is (some #{"leader_election_requests"} (registry-names reg)))
      (is (some #{"leader_election_request_duration_seconds"} (registry-names reg)))
      (is (some #{"leader_election_errors"} (registry-names reg))))))
