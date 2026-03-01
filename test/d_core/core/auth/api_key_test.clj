(ns d-core.core.auth.api-key-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.rate-limit.protocol :as rate-limit]
            [d-core.core.auth.api-key :as sut]))

(defn- fake-rate-limiter
  [consume-rate-limit-fn]
  (reify rate-limit/RateLimitProtocol
    (consume! [_ key opts]
      (consume-rate-limit-fn key opts))
    (consume-blocking! [_ key opts]
      (consume-rate-limit-fn key opts))))

(deftest bypasses-non-api-key-principal
  (testing "middleware ignores requests without api-key principal"
    (let [limiter (fake-rate-limiter (fn [_ _] (throw (ex-info "should not call" {}))))
          called? (atom false)
          handler (fn [_] (reset! called? true) {:status 200})
          wrapped (sut/wrap-api-key-limitations handler limiter {})
          resp (wrapped {:auth/principal {:subject "user-1" :auth-type :jwt}})]
      (is (= 200 (:status resp)))
      (is (true? @called?)))))

(deftest denies-method-not-allowed
  (testing "returns 403 when method does not match allowlist"
    (let [limiter (fake-rate-limiter (fn [_ _] {:allowed? true}))
          handler (fn [_] {:status 200})
          wrapped (sut/wrap-api-key-limitations handler limiter {})
          resp (wrapped {:request-method :post
                         :uri "/messages"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:method-allowlist [:get]}}})]
      (is (= 403 (:status resp)))
      (is (= "method-not-allowed" (get-in resp [:headers "X-Auth-Reason"])))))) 

(deftest denies-path-not-allowed
  (testing "returns 403 when path does not match allowlist"
    (let [limiter (fake-rate-limiter (fn [_ _] {:allowed? true}))
          handler (fn [_] {:status 200})
          wrapped (sut/wrap-api-key-limitations handler limiter {})
          resp (wrapped {:request-method :get
                         :uri "/admin"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:path-allowlist ["/messages/*"]}}})]
      (is (= 403 (:status resp)))
      (is (= "path-not-allowed" (get-in resp [:headers "X-Auth-Reason"])))))) 

(deftest denies-when-rate-limited
  (testing "returns 429 with retry headers"
    (let [limiter (fake-rate-limiter (fn [_ _]
                                       {:allowed? false
                                        :remaining 0
                                        :reset-at 1000
                                        :retry-after-ms 1500}))
          handler (fn [_] {:status 200})
          wrapped (sut/wrap-api-key-limitations handler limiter {})
          resp (wrapped {:request-method :get
                         :uri "/messages"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:rate-limit {:limit 1 :window-ms 60000}}}})]
      (is (= 429 (:status resp)))
      (is (= "2" (get-in resp [:headers "Retry-After"])))
      (is (= "0" (get-in resp [:headers "X-RateLimit-Remaining"])))))) 

(deftest allows-when-rate-limit-has-capacity
  (testing "delegates to handler when request is allowed"
    (let [limiter (fake-rate-limiter (fn [_ _]
                                       {:allowed? true
                                        :remaining 9
                                        :reset-at 1000
                                        :retry-after-ms nil}))
          called? (atom false)
          handler (fn [_] (reset! called? true) {:status 202})
          wrapped (sut/wrap-api-key-limitations handler limiter {})
          resp (wrapped {:request-method :get
                         :uri "/messages"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:rate-limit {:limit 10 :window-ms 60000}}}})]
      (is (= 202 (:status resp)))
      (is (true? @called?)))))

(deftest fail-open-on-rate-limiter-error
  (testing "allows request when limiter errors and fail-open is enabled"
    (let [limiter (fake-rate-limiter (fn [_ _] (throw (ex-info "limiter down" {}))))
          called? (atom false)
          handler (fn [_] (reset! called? true) {:status 200})
          wrapped (sut/wrap-api-key-limitations handler limiter {:rate-limit-fail-open? true})
          resp (wrapped {:request-method :get
                         :uri "/messages"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:rate-limit {:limit 1 :window-ms 60000}}}})]
      (is (= 200 (:status resp)))
      (is (true? @called?)))))

(deftest fail-closed-on-rate-limiter-error
  (testing "returns 503 when limiter errors and fail-open is disabled"
    (let [limiter (fake-rate-limiter (fn [_ _] (throw (ex-info "limiter down" {}))))
          handler (fn [_] {:status 200})
          wrapped (sut/wrap-api-key-limitations handler limiter {})
          resp (wrapped {:request-method :get
                         :uri "/messages"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:rate-limit {:limit 1 :window-ms 60000}}}})]
      (is (= 503 (:status resp)))))) 
