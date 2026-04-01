(ns d-core.integration.geocoding-nominatim-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.geocoding.nominatim :as nominatim]
            [d-core.core.geocoding.protocol :as p]
            [d-core.core.http.client :as http]))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_GEOCODING"))))

(defn- nominatim-url
  []
  (or (System/getenv "DCORE_NOMINATIM_URL")
      "http://localhost:8088"))

(defn- make-geocoder
  []
  (nominatim/->NominatimGeocoder
   (http/make-client {:id :nominatim
                      :base-url (nominatim-url)
                      :http-opts {:socket-timeout 15000
                                  :conn-timeout 15000}})
   "d-core geocoding integration tests"
   nil))

(deftest nominatim-forward-geocode
  (testing "local Nominatim resolves a known Monaco street"
    (if-not (integration-enabled?)
      (is true "Skipping Nominatim integration test; set INTEGRATION=1")
      (let [result (p/geocode (make-geocoder)
                              {:text "Avenue Pasteur, Monaco"
                               :limit 1}
                              {})]
        (is (= :nominatim (:provider result)))
        (is (= 1 (count (:items result))))
        (is (= "Avenue Pasteur"
               (get-in result [:items 0 :components :street])))
        (is (= "Monaco"
               (get-in result [:items 0 :components :country])))
        (is (= "mc"
               (get-in result [:items 0 :components :country-code])))))))

(deftest nominatim-reverse-geocode
  (testing "local Nominatim reverse geocodes a known Monaco coordinate"
    (if-not (integration-enabled?)
      (is true "Skipping Nominatim integration test; set INTEGRATION=1")
      (let [result (p/reverse-geocode (make-geocoder)
                                      {:lat 43.7334
                                       :lon 7.4221}
                                      {})]
        (is (= :nominatim (:provider result)))
        (is (= 1 (count (:items result))))
        (is (= "Monaco"
               (get-in result [:items 0 :components :country])))
        (is (= "mc"
               (get-in result [:items 0 :components :country-code])))
        (is (string? (get-in result [:items 0 :formatted-address])))))))

(deftest nominatim-no-result
  (testing "local Nominatim returns an empty result for a clearly invalid query"
    (if-not (integration-enabled?)
      (is true "Skipping Nominatim integration test; set INTEGRATION=1")
      (let [result (p/geocode (make-geocoder)
                              {:text "notarealplacezzzzzzzz"}
                              {})]
        (is (= :nominatim (:provider result)))
        (is (empty? (:items result)))))))
