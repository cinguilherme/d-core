(ns d-core.core.routing.schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.routing.schema :as schema]))

(deftest route-request-validation
  (testing "route requests validate and receive defaults"
    (is (= {:locations [{:lat 43.7334 :lon 7.4221}
                        {:lat 43.7299 :lon 7.4117}]
            :profile :driving
            :overview :simplified
            :include-geometry true
            :units :kilometers}
           (schema/validate-route-request!
            {:locations [{:lat 43.7334 :lon 7.4221}
                         {:lat 43.7299 :lon 7.4117}]}))))

  (testing "route requests reject unknown keys under strict mode"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-route-request!
                  {:locations [{:lat 43.7334 :lon 7.4221}
                               {:lat 43.7299 :lon 7.4117}]
                   :unexpected true}))))

  (testing "route requests require at least two locations"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-route-request!
                  {:locations [{:lat 43.7334 :lon 7.4221}]})))))

(deftest matrix-request-validation
  (testing "matrix requests normalize annotations and defaults"
    (is (= {:sources [{:lat 43.7334 :lon 7.4221}]
            :targets [{:lat 43.7299 :lon 7.4117}]
            :profile :driving
            :annotations [:duration :distance]
            :units :kilometers
            :verbose false}
           (schema/validate-matrix-request!
            {:sources [{:lat 43.7334 :lon 7.4221}]
             :targets [{:lat 43.7299 :lon 7.4117}]
             :annotations [:duration :distance :duration]}))))

  (testing "matrix requests reject unknown keys under strict mode"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-matrix-request!
                  {:sources [{:lat 43.7334 :lon 7.4221}]
                   :targets [{:lat 43.7299 :lon 7.4117}]
                   :unexpected true})))))

(deftest result-envelope-validation
  (testing "route envelopes validate"
    (is (= {:routes [{:distance 1200.0
                      :duration 180.0
                      :geometry {:format :polyline6
                                 :value "abc"}
                      :provider {:name :osrm
                                 :route-id "r1"}}]
            :provider :osrm}
           (schema/validate-route-envelope!
            {:routes [{:distance 1200.0
                       :duration 180.0
                       :geometry {:format :polyline6
                                  :value "abc"}
                       :provider {:name :osrm
                                  :route-id "r1"}}]
             :provider :osrm}))))

  (testing "matrix envelopes validate with nil unreachable cells"
    (is (= {:durations [[10.0 nil] [20.0 30.0]]
            :sources [{:location {:lat 43.7334 :lon 7.4221}}]
            :targets [{:location {:lat 43.7299 :lon 7.4117}}
                      {:location {:lat 43.7350 :lon 7.4200}}]
            :provider :osrm}
           (schema/validate-matrix-envelope!
            {:durations [[10.0 nil] [20.0 30.0]]
             :sources [{:location {:lat 43.7334 :lon 7.4221}}]
             :targets [{:location {:lat 43.7299 :lon 7.4117}}
                       {:location {:lat 43.7350 :lon 7.4200}}]
             :provider :osrm}))))

  (testing "result envelopes reject unknown keys under strict mode"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-route-envelope!
                  {:routes []
                   :provider :osrm
                   :unexpected true})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-matrix-envelope!
                  {:durations [[1.0]]
                   :sources [{:location {:lat 1.0 :lon 2.0}}]
                   :targets [{:location {:lat 1.0 :lon 2.0}}]
                   :provider :osrm
                   :unexpected true})))))
