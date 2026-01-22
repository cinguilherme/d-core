(ns d-core.core.auth.http-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.auth.http :as auth-http]
            [d-core.core.authn.protocol :as authn]
            [d-core.core.authz.protocol :as authz]))

(deftest wrap-authentication-success
  (testing "Adds principal and token to request"
    (let [captured (atom nil)
          authenticator (reify authn/Authenticator
                          (authenticate [_ _ _]
                            {:principal {:subject "user-1"}
                             :token "token-123"})
                          (verify-token [_ _ _] nil)
                          (challenge [_ _ _] {:status 401}))
          handler (fn [req]
                    (reset! captured req)
                    {:status 200})
          wrapped (auth-http/wrap-authentication handler authenticator {})]
      (is (= 200 (:status (wrapped {:headers {}}))))
      (is (= {:subject "user-1"} (:auth/principal @captured)))
      (is (= "token-123" (:auth/token @captured))))))

(deftest wrap-authentication-denies-by-default
  (testing "Uses challenge response when authentication fails"
    (let [authenticator (reify authn/Authenticator
                          (authenticate [_ _ _]
                            (throw (ex-info "bad" {:type :bad})))
                          (verify-token [_ _ _] nil)
                          (challenge [_ _ _] {:status 401 :body "Unauthorized"}))
          handler (fn [_] {:status 200})
          wrapped (auth-http/wrap-authentication handler authenticator {})]
      (is (= 401 (:status (wrapped {:headers {}})))))))

(deftest wrap-authentication-allows-anonymous
  (testing "Allows request through when allow-anonymous? is true"
    (let [captured (atom nil)
          authenticator (reify authn/Authenticator
                          (authenticate [_ _ _]
                            (throw (ex-info "bad" {:type :bad})))
                          (verify-token [_ _ _] nil)
                          (challenge [_ _ _] {:status 401}))
          handler (fn [req]
                    (reset! captured req)
                    {:status 200})
          wrapped (auth-http/wrap-authentication handler authenticator
                                                 {:allow-anonymous? true})]
      (is (= 200 (:status (wrapped {:headers {}}))))
      (is (= {:type :bad} (:auth/error @captured))))))

(deftest wrap-authorization-allows
  (testing "Allows request when authorizer permits"
    (let [captured (atom nil)
          authorizer (reify authz/Authorizer
                       (authorize [_ _ _ _] {:allowed? true}))
          handler (fn [req]
                    (reset! captured req)
                    {:status 200})
          wrapped (auth-http/wrap-authorization handler authorizer {})
          res (wrapped {:auth/principal {:subject "user-1"}
                        :auth/require {:scopes #{"messages:read"}}})]
      (is (= 200 (:status res)))
      (is (= true (get-in @captured [:auth/decision :allowed?]))))))

(deftest wrap-authorization-denies-with-default-response
  (testing "Returns 403/401 when authorization fails"
    (let [authorizer (reify authz/Authorizer
                       (authorize [_ _ _ _]
                         {:allowed? false :reason :missing-scope}))
          handler (fn [_] {:status 200})
          wrapped (auth-http/wrap-authorization handler authorizer {})
          res-auth (wrapped {:auth/principal {:subject "user-1"}
                             :auth/require {:scopes #{"admin"}}})
          res-unauth (wrapped {:auth/require {:scopes #{"admin"}}})]
      (is (= 403 (:status res-auth)))
      (is (= 401 (:status res-unauth))))))
