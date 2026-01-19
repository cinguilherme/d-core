(ns d-core.libs.time-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.libs.time :as time])
  (:import [java.time Instant ZoneId Duration]))

(def ^:private base-instant
  (Instant/parse "2024-01-01T00:00:00Z"))

(def ^:private base-map
  {:epoch-ms (.toEpochMilli base-instant)})

(def ^:private fixed-clock
  (time/new-clock {:type :fixed
                   :instant base-instant
                   :zone "UTC"}))

(deftest instant-shapes-and-coercion
  (testing "instant map detection"
    (is (time/instant-map? {:epoch-ms 1}))
    (is (time/instant-map? {:epoch-second 1 :nano 2}))
    (is (not (time/instant-map? {:epoch-second 1})))
    (is (not (time/instant-map? {:foo 1}))))
  (testing "instant predicate"
    (is (time/instant? base-instant))
    (is (not (time/instant? base-map)))))

(deftest instant-maps-roundtrip
  (testing "instant -> map -> instant"
    (is (time/instant= base-instant (time/map->instant (time/instant->map base-instant)))))
  (testing "epoch-second/nano map coercion"
    (let [m {:epoch-second 1 :nano 2}]
      (is (time/instant= (Instant/ofEpochSecond 1 2) (time/map->instant m)))))
  (testing "instant map includes zone when provided"
    (is (= "UTC" (:zone (time/instant->map base-instant (ZoneId/of "UTC")))))))

(deftest durations
  (testing "duration from unit"
    (is (= (Duration/ofSeconds 2) (time/duration 2 :seconds))))
  (testing "duration from map"
    (is (= (Duration/ofMillis 1500) (time/duration {:seconds 1 :millis 500}))))
  (testing "duration to map"
    (is (= {:millis 1500} (time/duration->map (Duration/ofMillis 1500))))))

(deftest clocks-and-now
  (testing "fixed clock returns expected instant"
    (is (= base-instant (time/now fixed-clock))))
  (testing "now-map includes zone"
    (let [m (time/now-map fixed-clock)]
      (is (= "UTC" (:zone m)))
      (is (= (:epoch-ms base-map) (:epoch-ms m))))))

(deftest plus-minus
  (testing "plus on instant"
    (is (= (Instant/parse "2024-01-01T00:00:30Z")
           (time/plus base-instant :seconds 30))))
  (testing "minus on map preserves zone"
    (let [m (assoc base-map :zone "UTC")]
      (is (= (assoc base-map :zone "UTC")
             (time/plus m :seconds 0)))
      (is (= "UTC" (:zone (time/minus m :seconds 1))))))
  (testing "plus on clock uses now"
    (is (= (Instant/parse "2024-01-01T00:00:10Z")
           (time/plus fixed-clock :seconds 10)))))

(deftest since-duration
  (let [later-clock (time/new-clock {:type :fixed
                                     :instant (Instant/parse "2024-01-01T00:00:10Z")
                                     :zone "UTC"})]
    (is (= (Duration/ofSeconds 10)
           (time/since later-clock base-instant)))))

(deftest instant-comparisons
  (let [later (Instant/parse "2024-01-01T00:00:10Z")]
    (testing "compare-instants"
      (is (= -1 (time/compare-instants base-instant later)))
      (is (= 0 (time/compare-instants base-map base-map)))
      (is (= 1 (time/compare-instants later base-instant))))
    (testing "before/after/equals"
      (is (true? (time/before? base-map later)))
      (is (true? (time/after? later base-map)))
      (is (true? (time/instant= base-map base-instant))))
    (testing "between inclusive"
      (is (true? (time/between? base-instant base-map later)))
      (is (true? (time/between? later base-instant later)))
      (is (false? (time/between? base-instant later (Instant/parse "2024-01-01T00:00:20Z")))))))
