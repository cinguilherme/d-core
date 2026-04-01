(ns d-core.core.geocoding.nominatim-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.geocoding.nominatim :as nominatim]
            [d-core.core.geocoding.protocol :as p]
            [d-core.core.http.client :as http]))

(defn- make-http-client
  []
  (http/make-client {:id :nominatim
                     :base-url "http://nominatim.local"}))

(defn- make-geocoder
  []
  (nominatim/->NominatimGeocoder (make-http-client) "d-core-tests" nil))

(deftest free-form-geocode-normalization
  (testing "free-form searches compile query params and normalize feature collections"
    (let [request* (atom nil)
          geocoder (make-geocoder)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:type "FeatureCollection"
                                             :geocoding {:version "0.1.0"
                                                         :attribution "OSM"
                                                         :licence "ODbL"
                                                         :query "Avenue Pasteur, Monaco"}
                                             :features [{:type "Feature"
                                                         :properties {:geocoding {:place_id 74700364
                                                                                  :osm_type "way"
                                                                                  :osm_id 92627436
                                                                                  :osm_key "highway"
                                                                                  :osm_value "tertiary"
                                                                                  :type "street"
                                                                                  :label "Avenue Pasteur, Jardin Exotique, Monaco, 98020, Monaco"
                                                                                  :name "Avenue Pasteur"
                                                                                  :postcode "98020"
                                                                                  :district "Jardin Exotique"
                                                                                  :city "Monaco"
                                                                                  :country "Monaco"
                                                                                  :country_code "mc"
                                                                                  :admin {:level10 "Jardin Exotique"}}}
                                                         :geometry {:type "Point"
                                                                    :coordinates [7.411691 43.7299652]}}]})})]
        (let [result (p/geocode geocoder
                                {:text "Avenue Pasteur, Monaco"
                                 :limit 1
                                 :language "en"
                                 :country-codes ["MC"]}
                                {})]
          (is (= "/search" (:path @request*)))
          (is (= "Avenue Pasteur, Monaco" (get-in @request* [:query-params :q])))
          (is (= 1 (get-in @request* [:query-params :limit])))
          (is (= "en" (get-in @request* [:query-params :accept-language])))
          (is (= "mc" (get-in @request* [:query-params :countrycodes])))
          (is (= :nominatim (:provider result)))
          (is (= "Avenue Pasteur, Jardin Exotique, Monaco, 98020, Monaco"
                 (get-in result [:items 0 :formatted-address])))
          (is (= {:lat 43.7299652 :lon 7.411691}
                 (get-in result [:items 0 :location])))
          (is (= "mc"
                 (get-in result [:items 0 :components :country-code])))
          (is (= "74700364"
                 (get-in result [:items 0 :provider :place-id]))))))))

(deftest structured-geocode-query
  (testing "structured search compiles address fields for Nominatim"
    (let [request* (atom nil)
          geocoder (make-geocoder)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:type "FeatureCollection"
                                             :geocoding {:licence "ODbL"}
                                             :features []})})]
        (is (= [] (:items (p/geocode geocoder
                                     {:address {:house-number "36"
                                                :street "Route de la Piscine"
                                                :city "Monaco"
                                                :country "Monaco"}}
                                     {}))))
        (is (= "36 Route de la Piscine"
               (get-in @request* [:query-params :street])))
        (is (= "Monaco" (get-in @request* [:query-params :city])))
        (is (= "Monaco" (get-in @request* [:query-params :country])))))))

(deftest reverse-geocode-normalization
  (testing "reverse geocode normalizes a single feature result"
    (let [request* (atom nil)
          geocoder (make-geocoder)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:type "FeatureCollection"
                                             :geocoding {:version "0.1.0"
                                                         :attribution "OSM"
                                                         :licence "ODbL"}
                                             :features {:type "Feature"
                                                        :properties {:geocoding {:place_id 74289470
                                                                                 :osm_type "node"
                                                                                 :osm_id 3574643941
                                                                                 :type "house"
                                                                                 :label "Explorers Pub, 36, Route de la Piscine, La Condamine, Monaco, 98000, Monaco"
                                                                                 :name "Explorers Pub"
                                                                                 :housenumber "36"
                                                                                 :postcode "98000"
                                                                                 :street "Route de la Piscine"
                                                                                 :district "La Condamine"
                                                                                 :city "Monaco"
                                                                                 :country "Monaco"
                                                                                 :country_code "mc"}}
                                                        :geometry {:type "Point"
                                                                   :coordinates [7.4222795 43.7335238]}}})})]
        (let [result (p/reverse-geocode geocoder {:lat 43.7334 :lon 7.4221} {})]
          (is (= "/reverse" (:path @request*)))
          (is (= 43.7334 (get-in @request* [:query-params :lat])))
          (is (= 7.4221 (get-in @request* [:query-params :lon])))
          (is (= "Route de la Piscine"
                 (get-in result [:items 0 :components :street])))
          (is (= "Monaco"
                 (get-in result [:items 0 :components :country]))))))))

(deftest empty-and-no-result-normalization
  (testing "search returns an empty envelope when no features are present"
    (let [geocoder (make-geocoder)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:type "FeatureCollection"
                                             :geocoding {:licence "ODbL"}
                                             :features []})})]
        (is (= {:items []
                :provider :nominatim
                :metadata {:licence "ODbL"}}
               (p/geocode geocoder {:text "notarealplacezzzzzzzz"} {}))))))

  (testing "reverse returns an empty envelope for no-result errors"
    (let [geocoder (make-geocoder)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:type "FeatureCollection"
                                             :geocoding {:licence "ODbL"}
                                             :error "Unable to geocode"})})]
        (is (= {:items []
                :provider :nominatim
                :metadata {:licence "ODbL"}}
               (p/reverse-geocode geocoder {:lat 0.0 :lon 0.0} {})))))))

(deftest malformed-provider-payloads-fail
  (testing "missing point geometry raises an actionable error"
    (let [geocoder (make-geocoder)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:type "FeatureCollection"
                                             :geocoding {:licence "ODbL"}
                                             :features [{:type "Feature"
                                                         :properties {:geocoding {:label "Broken"}}
                                                         :geometry {:type "LineString"
                                                                    :coordinates []}}]})})]
        (is (thrown? clojure.lang.ExceptionInfo
                     (p/geocode geocoder {:text "Broken"} {})))))))
