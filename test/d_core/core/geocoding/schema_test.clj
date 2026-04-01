(ns d-core.core.geocoding.schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.geocoding.schema :as schema]))

(deftest geocode-query-validation
  (testing "free-form queries validate"
    (is (= {:text "Avenue Pasteur, Monaco"
            :limit 1
            :language "en"
            :country-codes ["mc"]}
           (schema/validate-geocode-query!
            {:text "Avenue Pasteur, Monaco"
             :limit 1
             :language "en"
             :country-codes ["MC"]}))))

  (testing "structured queries normalize nils and country codes"
    (is (= {:address {:street "Route de la Piscine"
                      :city "Monaco"
                      :country "Monaco"}
            :country-codes ["fr" "mc"]}
           (schema/validate-geocode-query!
            {:address {:street "Route de la Piscine"
                       :city "Monaco"
                       :country "Monaco"
                       :state nil}
             :country-codes ["FR" nil "mc"]}))))

  (testing "unknown keys fail under strict validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-geocode-query!
                  {:text "Monaco"
                   :unexpected true}))))

  (testing "structured queries require at least one address component"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-geocode-query!
                  {:address {}})))))

(deftest reverse-geocode-input-validation
  (testing "reverse inputs validate"
    (is (= {:lat 43.7334
            :lon 7.4221
            :language "en"}
           (schema/validate-reverse-geocode-input!
            {:lat 43.7334
             :lon 7.4221
             :language "en"}))))

  (testing "unknown keys fail under strict validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-reverse-geocode-input!
                  {:lat 43.7334
                   :lon 7.4221
                   :zoom 18})))))

(deftest result-envelope-validation
  (testing "normalized geocoding envelopes validate"
    (is (= {:items [{:formatted-address "Avenue Pasteur, Monaco"
                     :location {:lat 43.7299652 :lon 7.411691}
                     :components {:street "Avenue Pasteur"
                                  :city "Monaco"
                                  :country "Monaco"
                                  :country-code "mc"}
                     :provider {:name :nominatim
                                :place-id "74700364"}}]
            :provider :nominatim}
           (schema/validate-result-envelope!
            {:items [{:formatted-address "Avenue Pasteur, Monaco"
                      :location {:lat 43.7299652 :lon 7.411691}
                      :components {:street "Avenue Pasteur"
                                   :city "Monaco"
                                   :country "Monaco"
                                   :country-code "mc"}
                      :provider {:name :nominatim
                                 :place-id "74700364"}}]
             :provider :nominatim}))))

  (testing "unknown keys fail under strict validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-result-envelope!
                  {:items []
                   :provider :nominatim
                   :unexpected true})))))
