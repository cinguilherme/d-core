(ns d-core.core.auth.api-key-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.api-keys.protocol :as api-keys]
            [d-core.core.auth.api-key :as sut]))

(defn- fake-store
  [consume-rate-limit-fn]
  (reify api-keys/ApiKeyStore
    (ensure-schema! [_ _] {:ok true})
    (create-key! [_ _ _] nil)
    (get-key [_ _ _] nil)
    (list-keys [_ _ _] [])
    (revoke-key! [_ _ _] {:revoked? false})
    (rotate-key! [_ _ _] nil)
    (authenticate-key [_ _ _] nil)
    (consume-rate-limit! [_ api-key-id opts] (consume-rate-limit-fn api-key-id opts))))

(deftest bypasses-non-api-key-principal
  (testing "middleware ignores requests without api-key principal"
    (let [store (fake-store (fn [_ _] (throw (ex-info "should not call" {}))))
          called? (atom false)
          handler (fn [_] (reset! called? true) {:status 200})
          wrapped (sut/wrap-api-key-limitations handler store {})
          resp (wrapped {:auth/principal {:subject "user-1" :auth-type :jwt}})]
      (is (= 200 (:status resp)))
      (is (true? @called?)))))

(deftest denies-method-not-allowed
  (testing "returns 403 when method does not match allowlist"
    (let [store (fake-store (fn [_ _] {:allowed? true}))
          handler (fn [_] {:status 200})
          wrapped (sut/wrap-api-key-limitations handler store {})
          resp (wrapped {:request-method :post
                         :uri "/messages"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:method-allowlist [:get]}}})]
      (is (= 403 (:status resp)))
      (is (= "method-not-allowed" (get-in resp [:headers "X-Auth-Reason"])))))) 

(deftest denies-path-not-allowed
  (testing "returns 403 when path does not match allowlist"
    (let [store (fake-store (fn [_ _] {:allowed? true}))
          handler (fn [_] {:status 200})
          wrapped (sut/wrap-api-key-limitations handler store {})
          resp (wrapped {:request-method :get
                         :uri "/admin"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:path-allowlist ["/messages/*"]}}})]
      (is (= 403 (:status resp)))
      (is (= "path-not-allowed" (get-in resp [:headers "X-Auth-Reason"])))))) 

(deftest denies-when-rate-limited
  (testing "returns 429 with retry headers"
    (let [store (fake-store (fn [_ _]
                              {:allowed? false
                               :remaining 0
                               :reset-at 1000
                               :retry-after-ms 1500}))
          handler (fn [_] {:status 200})
          wrapped (sut/wrap-api-key-limitations handler store {})
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
    (let [store (fake-store (fn [_ _]
                              {:allowed? true
                               :remaining 9
                               :reset-at 1000
                               :retry-after-ms nil}))
          called? (atom false)
          handler (fn [_] (reset! called? true) {:status 202})
          wrapped (sut/wrap-api-key-limitations handler store {})
          resp (wrapped {:request-method :get
                         :uri "/messages"
                         :auth/principal {:auth-type :api-key
                                          :api-key/id "ak-1"
                                          :api-key/limits {:rate-limit {:limit 10 :window-ms 60000}}}})]
      (is (= 202 (:status resp)))
      (is (true? @called?))))) 
