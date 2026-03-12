(ns d-core.core.geofence.tile38-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.tile38.client :as tc]
            [d-core.core.geofence.protocol :as gp]
            [d-core.core.geofence.tile38 :as geo]))

(deftest compile-static-fence-args
  (testing "static nearby fences compile into Tile38 hook args"
    (is (= ["NEARBY"
            "geo:tenant-1:actors"
            "MATCH"
            "driver:*"
            "WHEREIN"
            "status"
            1
            "available"
            "FENCE"
            "DETECT"
            "enter,exit"
            "COMMANDS"
            "del,set"
            "POINT"
            40.71
            -74.0
            750]
           (geo/compile-fence-args
            {:id "airport-zone"
             :kind :static
             :source {:key "geo:tenant-1:actors"
                      :match "driver:*"
                      :filters [{:kind :eq :field "status" :value "available"}]}
             :shape {:kind :circle
                     :center {:lat 40.71 :lon -74.0}
                     :radius-m 750}
             :operation :nearby
             :delivery {:endpoint "http://localhost:9898/hook"}
             :detect #{:enter :exit}
             :commands #{:set :del}})))))

(deftest compile-roam-fence-args
  (testing "roaming fences compile into Tile38 hook args"
    (is (= ["NEARBY"
            "geo:tenant-1:actors"
            "MATCH"
            "driver:*"
            "FENCE"
            "COMMANDS"
            "del,set"
            "NODWELL"
            "ROAM"
            "geo:tenant-1:actors"
            "passenger:*"
            3000]
           (geo/compile-fence-args
            {:id "dispatch-candidate"
             :kind :roam
             :source {:key "geo:tenant-1:actors"
                      :match "driver:*"}
             :target {:key "geo:tenant-1:actors"
                      :pattern "passenger:*"
                      :radius-m 3000}
             :delivery {:endpoint "http://localhost:9898/hook"}
             :detect #{:roam}
             :commands #{:set :del}
             :no-dwell? true})))))

(deftest upsert-fence-delegates-to-client
  (testing "upsert-fence! validates and calls SETHOOK"
    (let [calls (atom [])
          geofence (geo/->Tile38Geofence :client)]
      (with-redefs [tc/sethook! (fn [client name endpoint args opts]
                                  (swap! calls conj {:client client
                                                     :name name
                                                     :endpoint endpoint
                                                     :args args
                                                     :opts opts})
                                  {:ok true})]
        (is (= {:ok true}
               (gp/upsert-fence! geofence
                                 {:id "airport-zone"
                                  :kind :static
                                  :source {:key "geo:tenant-1:actors"}
                                  :shape {:kind :circle
                                          :center {:lat 40.71 :lon -74.0}
                                          :radius-m 750}
                                  :operation :nearby
                                  :delivery {:endpoint "http://localhost:9898/hook"}}
                                 {:output :json})))
        (is (= [{:client :client
                 :name "airport-zone"
                 :endpoint "http://localhost:9898/hook"
                 :args ["NEARBY" "geo:tenant-1:actors" "FENCE" "POINT" 40.71 -74.0 750]
                 :opts {:output :json}}]
               @calls))))))

(deftest test-fence-compiles-or-executes
  (testing "test-fence uses Tile38 TEST for static within fences when object context is provided"
    (let [calls (atom [])
          geofence (geo/->Tile38Geofence :client)]
      (with-redefs [tc/test! (fn [client key id args opts]
                               (swap! calls conj {:client client
                                                  :key key
                                                  :id id
                                                  :args args
                                                  :opts opts})
                               {:ok true :result true})]
        (is (= {:ok true
                :compiled ["WITHIN" "geo:tenant-1:fences" "FENCE" "OBJECT" "{\"type\":\"Polygon\",\"coordinates\":[]}"]
                :result {:ok true :result true}}
               (gp/test-fence geofence
                              {:id "polygon-zone"
                               :kind :static
                               :source {:key "geo:tenant-1:fences"}
                               :shape {:kind :geojson
                                       :geometry {:type "Polygon"
                                                  :coordinates []}}
                               :operation :within
                               :delivery {:endpoint "http://localhost:9898/hook"}}
                              {:key "geo:tenant-1:actors"
                               :id "driver:123"})))
        (is (= [{:client :client
                 :key "geo:tenant-1:actors"
                 :id "driver:123"
                 :args ["WITHIN" "OBJECT" "{\"type\":\"Polygon\",\"coordinates\":[]}"]
                 :opts {:key "geo:tenant-1:actors" :id "driver:123"}}]
               @calls))))))

(deftest normalize-event-shapes
  (testing "static Tile38 payloads normalize into canonical events"
    (let [geofence (geo/->Tile38Geofence :client)]
      (is (= {:backend :tile38
              :fence-id "airport-zone"
              :fence-kind :static
              :detect :enter
              :command :set
              :subject {:key "geo:tenant-1:actors"
                        :id "driver:123"
                        :geometry {:lat 40.71 :lon -74.0}
                        :fields {:kind "driver"}}
              :provider {:hook "airport-zone"
                         :group "fence"
                         :time "1741773232"
                         :raw {:hook "airport-zone"
                               :group "fence"
                               :detect "enter"
                               :command "set"
                               :key "geo:tenant-1:actors"
                               :id "driver:123"
                               :fields {:kind "driver"}
                               :object {:type "Point" :coordinates [-74.0 40.71]}
                               :time "1741773232"}}}
             (gp/normalize-event geofence
                                 {:hook "airport-zone"
                                  :group "fence"
                                  :detect "enter"
                                  :command "set"
                                  :key "geo:tenant-1:actors"
                                  :id "driver:123"
                                  :fields {:kind "driver"}
                                  :object {:type "Point" :coordinates [-74.0 40.71]}
                                  :time "1741773232"}
                                 {})))))

  (testing "roaming payloads populate counterparty and detect roam"
    (let [geofence (geo/->Tile38Geofence :client)]
      (is (= :roam
             (:detect
              (gp/normalize-event geofence
                                  "{\"hook\":\"dispatch-candidate\",\"detect\":\"roam\",\"command\":\"set\",\"key\":\"geo:tenant-1:actors\",\"id\":\"driver:123\",\"fields\":{\"kind\":\"driver\"},\"nearby\":{\"id\":\"passenger:999\",\"fields\":{\"kind\":\"passenger\"},\"meters\":125.0}}"
                                  {:validate? true}))))
      (is (= {:key "geo:tenant-1:actors"
              :id "passenger:999"
              :fields {:kind "passenger"}
              :distance-m 125.0}
             (:counterparty
              (gp/normalize-event geofence
                                  "{\"hook\":\"dispatch-candidate\",\"detect\":\"roam\",\"command\":\"set\",\"key\":\"geo:tenant-1:actors\",\"id\":\"driver:123\",\"fields\":{\"kind\":\"driver\"},\"nearby\":{\"id\":\"passenger:999\",\"fields\":{\"kind\":\"passenger\"},\"meters\":125.0}}"
                                  {:validate? true})))))))

(deftest fence-listing-and-lookup
  (testing "list-fences and get-fence normalize HOOKS results"
    (let [geofence (geo/->Tile38Geofence :client)]
      (with-redefs [tc/hooks! (fn [_ _]
                                {:ok true
                                 :cursor 0
                                 :hooks [{:hook "airport-zone"}
                                         {:hook "dispatch-candidate"}]})]
        (is (= {:ok true
                :items [{:hook "airport-zone"}
                        {:hook "dispatch-candidate"}]
                :cursor 0
                :raw {:ok true
                      :cursor 0
                      :hooks [{:hook "airport-zone"}
                              {:hook "dispatch-candidate"}]}}
               (gp/list-fences geofence {} {})))
        (is (= {:ok true
                :item {:hook "dispatch-candidate"}
                :raw {:ok true
                      :cursor 0
                      :hooks [{:hook "airport-zone"}
                              {:hook "dispatch-candidate"}]}}
               (gp/get-fence geofence "dispatch-candidate" {})))))))
