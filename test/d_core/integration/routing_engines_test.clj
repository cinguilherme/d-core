(ns d-core.integration.routing-engines-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.http.client :as http]
            [d-core.core.routing.osrm :as osrm]
            [d-core.core.routing.protocol :as p]
            [d-core.core.routing.valhalla :as valhalla]))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_ROUTING"))))

(defn- osrm-url
  []
  (or (System/getenv "DCORE_OSRM_URL")
      "http://localhost:5001"))

(defn- valhalla-url
  []
  (or (System/getenv "DCORE_VALHALLA_URL")
      "http://localhost:8002"))

(defn- make-osrm-router
  []
  (osrm/->OsrmRouter
   (http/make-client {:id :osrm
                      :base-url (osrm-url)
                      :http-opts {:socket-timeout 30000
                                  :conn-timeout 30000}})
   "d-core routing integration tests"))

(defn- make-valhalla-router
  []
  (valhalla/->ValhallaRouter
   (http/make-client {:id :valhalla
                      :base-url (valhalla-url)
                      :http-opts {:socket-timeout 30000
                                  :conn-timeout 30000}})
   "d-core routing integration tests"))

(deftest osrm-route
  (testing "OSRM resolves a known Monaco driving route"
    (if-not (integration-enabled?)
      (is true "Skipping routing integration test; set DCORE_INTEGRATION_ROUTING=1")
      (let [result (p/route (make-osrm-router)
                            {:locations [{:lat 43.7334 :lon 7.4221}
                                         {:lat 43.7299 :lon 7.4117}]}
                            {})]
        (is (= :osrm (:provider result)))
        (is (pos? (count (:routes result))))
        (is (pos? (get-in result [:routes 0 :distance])))
        (is (pos? (get-in result [:routes 0 :duration])))))))

(deftest osrm-matrix
  (testing "OSRM resolves a small Monaco matrix"
    (if-not (integration-enabled?)
      (is true "Skipping routing integration test; set DCORE_INTEGRATION_ROUTING=1")
      (let [result (p/matrix (make-osrm-router)
                             {:sources [{:lat 43.7334 :lon 7.4221}
                                        {:lat 43.7321 :lon 7.4211}]
                              :targets [{:lat 43.7299 :lon 7.4117}
                                        {:lat 43.7310 :lon 7.4170}]}
                             {})]
        (is (= :osrm (:provider result)))
        (is (= 2 (count (:durations result))))
        (is (= 2 (count (first (:durations result)))))
        (is (number? (get-in result [:durations 0 0])))))))

(deftest osrm-out-of-coverage
  (testing "OSRM handles out-of-coverage coordinates without transport errors"
    (if-not (integration-enabled?)
      (is true "Skipping routing integration test; set DCORE_INTEGRATION_ROUTING=1")
      (let [result (p/route (make-osrm-router)
                            {:locations [{:lat 0.0 :lon 0.0}
                                         {:lat 0.1 :lon 0.1}]}
                            {})]
        (is (= :osrm (:provider result)))
        (is (vector? (:routes result)))))))

(deftest valhalla-route
  (testing "Valhalla resolves a known Monaco driving route"
    (if-not (integration-enabled?)
      (is true "Skipping routing integration test; set DCORE_INTEGRATION_ROUTING=1")
      (let [result (p/route (make-valhalla-router)
                            {:locations [{:lat 43.7334 :lon 7.4221}
                                         {:lat 43.7299 :lon 7.4117}]}
                            {})]
        (is (= :valhalla (:provider result)))
        (is (pos? (count (:routes result))))
        (is (pos? (get-in result [:routes 0 :distance])))
        (is (pos? (get-in result [:routes 0 :duration])))))))

(deftest valhalla-matrix
  (testing "Valhalla resolves a small Monaco matrix"
    (if-not (integration-enabled?)
      (is true "Skipping routing integration test; set DCORE_INTEGRATION_ROUTING=1")
      (let [result (p/matrix (make-valhalla-router)
                             {:sources [{:lat 43.7334 :lon 7.4221}
                                        {:lat 43.7321 :lon 7.4211}]
                              :targets [{:lat 43.7299 :lon 7.4117}
                                        {:lat 43.7310 :lon 7.4170}]}
                             {})]
        (is (= :valhalla (:provider result)))
        (is (= 2 (count (:durations result))))
        (is (= 2 (count (first (:durations result)))))
        (is (number? (get-in result [:durations 0 0])))))))

(deftest valhalla-no-route
  (testing "Valhalla normalizes no-route responses to empty routes"
    (if-not (integration-enabled?)
      (is true "Skipping routing integration test; set DCORE_INTEGRATION_ROUTING=1")
      (let [result (p/route (make-valhalla-router)
                            {:locations [{:lat 0.0 :lon 0.0}
                                         {:lat 0.1 :lon 0.1}]}
                            {})]
        (is (= :valhalla (:provider result)))
        (is (empty? (:routes result)))))))
