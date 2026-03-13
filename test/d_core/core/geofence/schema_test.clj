(ns d-core.core.geofence.schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.geofence.schema :as gfs]))

(deftest tracked-object-validation
  (testing "point tracked objects validate"
    (is (= {:key "geo:tenant-1:actors"
            :id "driver:123"
            :geometry {:kind :point
                       :point {:lat 40.71 :lon -74.00}}
            :fields {:kind "driver" :status "available"}
            :ttl-seconds 30}
           (gfs/validate-tracked-object!
            {:key "geo:tenant-1:actors"
             :id "driver:123"
             :geometry {:kind :point
                        :point {:lat 40.71 :lon -74.00}}
             :fields {:kind "driver" :status "available"}
             :ttl-seconds 30})))))

(deftest static-fence-validation
  (testing "static circle fences validate"
    (is (= {:id "airport-zone"
            :kind :static
            :source {:key "geo:tenant-1:actors"
                     :match "driver:*"
                     :filters [{:kind :eq :field "status" :value "available"}]}
            :shape {:kind :circle
                    :center {:lat 40.71 :lon -74.00}
                    :radius-m 750}
            :operation :nearby
            :delivery {:endpoint "kafka://broker-1/geofence.events"}
            :detect #{:enter :exit}
            :commands #{:set :del}}
           (gfs/validate-fence!
            {:id "airport-zone"
             :kind :static
             :source {:key "geo:tenant-1:actors"
                      :match "driver:*"
                      :filters [{:kind :eq :field "status" :value "available"}]}
             :shape {:kind :circle
                     :center {:lat 40.71 :lon -74.00}
                     :radius-m 750}
             :operation :nearby
             :delivery {:endpoint "kafka://broker-1/geofence.events"}
             :detect #{:enter :exit}
             :commands #{:set :del}}))))

  (testing "static geojson fences support multishape payloads"
    (is (= :static
           (:kind
            (gfs/validate-fence!
             {:id "city-zones"
              :kind :static
              :source {:key "geo:tenant-1:actors"}
              :shape {:kind :geojson
                      :geometry {:type "MultiPolygon"
                                 :coordinates []}}
              :operation :within
              :delivery {:endpoint "http://localhost:8080/hooks/geofence"}}))))))

(deftest roam-fence-validation
  (testing "roaming fences validate"
    (is (= {:id "dispatch-candidate"
            :kind :roam
            :source {:key "geo:tenant-1:actors"
                     :match "driver:*"}
            :target {:key "geo:tenant-1:actors"
                     :pattern "passenger:*"
                     :radius-m 3000}
            :delivery {:endpoint "rabbitmq://guest:guest@localhost/geofence"}
            :detect #{:roam}
            :commands #{:set :del}
            :no-dwell? true}
           (gfs/validate-fence!
            {:id "dispatch-candidate"
             :kind :roam
             :source {:key "geo:tenant-1:actors"
                      :match "driver:*"}
             :target {:key "geo:tenant-1:actors"
                      :pattern "passenger:*"
                      :radius-m 3000}
             :delivery {:endpoint "rabbitmq://guest:guest@localhost/geofence"}
             :detect #{:roam}
             :commands #{:set :del}
             :no-dwell? true}))))

  (testing "roaming fences reject static detect values"
    (is (thrown? clojure.lang.ExceptionInfo
                 (gfs/validate-fence!
                  {:id "dispatch-candidate"
                   :kind :roam
                   :source {:key "geo:tenant-1:actors"
                            :match "driver:*"}
                   :target {:key "geo:tenant-1:actors"
                            :pattern "passenger:*"
                            :radius-m 3000}
                   :delivery {:endpoint "rabbitmq://guest:guest@localhost/geofence"}
                   :detect #{:enter}})))))

(deftest event-validation
  (testing "static events validate"
    (is (= :static
           (:fence-kind
            (gfs/validate-event!
             {:backend :tile38
              :fence-id "airport-zone"
              :fence-kind :static
              :detect :enter
              :command :set
              :subject {:key "geo:tenant-1:actors"
                        :id "driver:123"
                        :geometry {:lat 40.71 :lon -74.00}
                        :fields {:kind "driver"}}
              :provider {:hook "airport-zone"
                         :group "fence"
                         :time 1741662000}})))))

  (testing "roaming events support counterparty payloads"
    (is (= :roam
           (:detect
            (gfs/validate-event!
             {:backend :tile38
              :fence-id "dispatch-candidate"
              :fence-kind :roam
              :detect :roam
              :command :set
              :subject {:key "geo:tenant-1:actors"
                        :id "driver:123"
                        :fields {:kind "driver"}}
              :counterparty {:key "geo:tenant-1:actors"
                             :id "passenger:999"
                             :fields {:kind "passenger"}
                             :distance-m 125.0}
              :provider {:hook "dispatch-candidate"
                         :raw {:detect "roam"}}})))))

  (testing "unknown keys fail under strict validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (gfs/validate-event!
                  {:backend :tile38
                   :fence-id "airport-zone"
                   :fence-kind :static
                   :detect :enter
                   :subject {:key "geo:tenant-1:actors"
                             :id "driver:123"}
                   :unexpected true})))))
