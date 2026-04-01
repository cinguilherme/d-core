(ns d-core.core.routing.valhalla-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.http.client :as http]
            [d-core.core.routing.protocol :as p]
            [d-core.core.routing.valhalla :as valhalla]))

(defn- make-http-client
  []
  (http/make-client {:id :valhalla
                     :base-url "http://valhalla.local"}))

(defn- make-router
  []
  (valhalla/->ValhallaRouter (make-http-client) "d-core-tests"))

(deftest route-normalization
  (testing "route requests serialize payload and normalize a Valhalla trip"
    (let [request* (atom nil)
          router (make-router)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:trip {:status 0
                                                    :summary {:length 1.5
                                                              :time 220}
                                                    :legs [{:summary {:length 1.5
                                                                      :time 220}
                                                            :shape "abc"}]}})})]
        (let [result (p/route router
                              {:locations [{:lat 43.7334 :lon 7.4221}
                                           {:lat 43.7299 :lon 7.4117}]
                               :steps true}
                              {})
              payload (json/parse-string (get-in @request* [:query-params :json]) true)]
          (is (= "/route" (:path @request*)))
          (is (= "auto" (:costing payload)))
          (is (= true (:narrative payload)))
          (is (= :valhalla (:provider result)))
          (is (= 1500.0 (get-in result [:routes 0 :distance])))
          (is (= 220.0 (get-in result [:routes 0 :duration])))
          (is (= "abc" (get-in result [:routes 0 :geometry :value]))))))))

(deftest matrix-normalization
  (testing "matrix requests normalize concise matrix payloads"
    (let [request* (atom nil)
          router (make-router)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:durations [[120.0 nil] [140.0 180.0]]
                                             :distances [[1.2 nil] [1.4 1.8]]
                                             :sources [{:name "S1" :lat 43.7334 :lon 7.4221}
                                                       {:name "S2" :lat 43.7321 :lon 7.4211}]
                                             :targets [{:name "T1" :lat 43.7299 :lon 7.4117}
                                                       {:name "T2" :lat 43.7310 :lon 7.4170}]})})]
        (let [result (p/matrix router
                               {:sources [{:lat 43.7334 :lon 7.4221}
                                          {:lat 43.7321 :lon 7.4211}]
                                :targets [{:lat 43.7299 :lon 7.4117}
                                          {:lat 43.7310 :lon 7.4170}]}
                               {})
              payload (json/parse-string (get-in @request* [:query-params :json]) true)]
          (is (= "/sources_to_targets" (:path @request*)))
          (is (= "auto" (:costing payload)))
          (is (= :valhalla (:provider result)))
          (is (= [[120.0 nil] [140.0 180.0]] (:durations result)))
          (is (= [[1200.0 nil] [1400.0 1800.0]] (:distances result)))
          (is (= "S1" (get-in result [:sources 0 :name])))
          (is (= "T2" (get-in result [:targets 1 :name]))))))))

(deftest matrix-verbose-normalization
  (testing "verbose matrix cell payloads map to row/column matrices"
    (let [router (make-router)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:sources_to_targets [{:from_index 0 :to_index 0 :time 10 :distance 0.5}
                                                                  {:from_index 0 :to_index 1 :time 20 :distance 0.7}
                                                                  {:from_index 1 :to_index 0 :time 30 :distance 0.9}
                                                                  {:from_index 1 :to_index 1 :time 40 :distance 1.1}]})})]
        (is (= {:durations [[10.0 20.0] [30.0 40.0]]
                :distances [[500.0 700.0] [900.0 1100.0]]
                :sources [{:location {:lat 43.7334 :lon 7.4221}}
                          {:location {:lat 43.7321 :lon 7.4211}}]
                :targets [{:location {:lat 43.7299 :lon 7.4117}}
                          {:location {:lat 43.731 :lon 7.417}}]
                :provider :valhalla
                :metadata {:units "kilometers"}}
               (update (p/matrix router
                                 {:sources [{:lat 43.7334 :lon 7.4221}
                                            {:lat 43.7321 :lon 7.4211}]
                                  :targets [{:lat 43.7299 :lon 7.4117}
                                            {:lat 43.7310 :lon 7.4170}]}
                                 {})
                       :metadata #(select-keys % [:units]))))))))

(deftest empty-normalization
  (testing "route no-path responses normalize to empty routes"
    (let [router (make-router)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 400
                                     :body (json/generate-string
                                            {:error_code 442
                                             :error "No path could be found for input"})})]
        (is (= {:routes []
                :provider :valhalla
                :metadata {:code 442
                           :message "No path could be found for input"}}
               (p/route router
                        {:locations [{:lat 0.0 :lon 0.0}
                                     {:lat 0.1 :lon 0.1}]}
                        {}))))))

  (testing "matrix no-path responses normalize to nil matrix cells"
    (let [router (make-router)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 400
                                     :body (json/generate-string
                                            {:error_code 442
                                             :error "No suitable edges near location"})})]
        (is (= {:durations [[nil nil] [nil nil]]
                :distances [[nil nil] [nil nil]]
                :sources [{:location {:lat 43.7334 :lon 7.4221}}
                          {:location {:lat 43.7321 :lon 7.4211}}]
                :targets [{:location {:lat 43.7299 :lon 7.4117}}
                          {:location {:lat 43.731 :lon 7.417}}]
                :provider :valhalla
                :metadata {:code 442
                           :message "No suitable edges near location"}}
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
                                            {:trip {:status 0
                                                    :summary {:length "abc" :time 20}
                                                    :legs []}})})]
        (is (thrown? clojure.lang.ExceptionInfo
                     (p/route router
                              {:locations [{:lat 43.7334 :lon 7.4221}
                                           {:lat 43.7299 :lon 7.4117}]}
                              {})))))))
