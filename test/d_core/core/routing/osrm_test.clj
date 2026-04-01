(ns d-core.core.routing.osrm-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.http.client :as http]
            [d-core.core.routing.osrm :as osrm]
            [d-core.core.routing.protocol :as p]))

(defn- make-http-client
  []
  (http/make-client {:id :osrm
                     :base-url "http://osrm.local"}))

(defn- make-router
  []
  (osrm/->OsrmRouter (make-http-client) "d-core-tests"))

(deftest route-normalization
  (testing "route requests compile path/query and normalize route envelopes"
    (let [request* (atom nil)
          router (make-router)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:code "Ok"
                                             :routes [{:distance 1498.1
                                                       :duration 253.4
                                                       :weight 253.4
                                                       :weight_name "routability"
                                                       :geometry "abc"
                                                       :legs [{:distance 1498.1
                                                               :duration 253.4
                                                               :summary "Boulevard Albert 1er"}]}]
                                             :waypoints [{:name "A"
                                                          :location [7.4221 43.7334]}]})})]
        (let [result (p/route router
                              {:locations [{:lat 43.7334 :lon 7.4221}
                                           {:lat 43.7299 :lon 7.4117}]
                               :steps true}
                              {})]
          (is (= "/route/v1/driving/7.4221,43.7334;7.4117,43.7299" (:path @request*)))
          (is (= "polyline6" (get-in @request* [:query-params :geometries])))
          (is (= true (get-in @request* [:query-params :steps])))
          (is (= :osrm (:provider result)))
          (is (= 1498.1 (get-in result [:routes 0 :distance])))
          (is (= "abc" (get-in result [:routes 0 :geometry :value])))
          (is (= "Boulevard Albert 1er"
                 (get-in result [:routes 0 :legs 0 :summary]))))))))

(deftest matrix-normalization
  (testing "matrix requests normalize durations and distances with snapped points"
    (let [request* (atom nil)
          router (make-router)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:code "Ok"
                                             :durations [[120.0 nil] [140.0 180.0]]
                                             :distances [[980.0 nil] [1100.0 1350.0]]
                                             :sources [{:name "S1" :location [7.4221 43.7334]}
                                                       {:name "S2" :location [7.4211 43.7321]}]
                                             :destinations [{:name "T1" :location [7.4117 43.7299]}
                                                            {:name "T2" :location [7.4170 43.7310]}]})})]
        (let [result (p/matrix router
                               {:sources [{:lat 43.7334 :lon 7.4221}
                                          {:lat 43.7321 :lon 7.4211}]
                                :targets [{:lat 43.7299 :lon 7.4117}
                                          {:lat 43.7310 :lon 7.4170}]}
                               {})]
          (is (= "/table/v1/driving/7.4221,43.7334;7.4211,43.7321;7.4117,43.7299;7.417,43.731"
                 (:path @request*)))
          (is (= "duration,distance" (get-in @request* [:query-params :annotations])))
          (is (= :osrm (:provider result)))
          (is (= [[120.0 nil] [140.0 180.0]] (:durations result)))
          (is (= [[980.0 nil] [1100.0 1350.0]] (:distances result)))
          (is (= "S1" (get-in result [:sources 0 :name])))
          (is (= "T2" (get-in result [:targets 1 :name]))))))))

(deftest empty-normalization
  (testing "NoRoute maps to empty routes"
    (let [router (make-router)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:code "NoRoute"
                                             :message "Impossible route"})})]
        (is (= {:routes []
                :provider :osrm
                :metadata {:code "NoRoute"
                           :message "Impossible route"}}
               (p/route router
                        {:locations [{:lat 0.0 :lon 0.0}
                                     {:lat 0.1 :lon 0.1}]}
                        {}))))))

  (testing "NoTable maps to nil matrix cells with request dimensions"
    (let [router (make-router)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:code "NoTable"
                                             :message "No table"})})]
        (is (= {:durations [[nil nil] [nil nil]]
                :distances [[nil nil] [nil nil]]
                :sources [{:location {:lat 43.7334 :lon 7.4221}}
                          {:location {:lat 43.7321 :lon 7.4211}}]
                :targets [{:location {:lat 43.7299 :lon 7.4117}}
                          {:location {:lat 43.7310 :lon 7.4170}}]
                :provider :osrm
                :metadata {:code "NoTable"
                           :message "No table"}}
               (p/matrix router
                         {:sources [{:lat 43.7334 :lon 7.4221}
                                    {:lat 43.7321 :lon 7.4211}]
                          :targets [{:lat 43.7299 :lon 7.4117}
                                    {:lat 43.7310 :lon 7.4170}]}
                         {})))))))

(deftest malformed-payloads-fail
  (testing "invalid route payloads raise ex-info"
    (let [router (make-router)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:code "Ok"
                                             :routes [{:distance "abc"
                                                       :duration 10}]})})]
        (is (thrown? clojure.lang.ExceptionInfo
                     (p/route router
                              {:locations [{:lat 43.7334 :lon 7.4221}
                                           {:lat 43.7299 :lon 7.4117}]}
                              {})))))))
