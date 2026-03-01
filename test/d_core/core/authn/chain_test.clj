(ns d-core.core.authn.chain-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.authn.chain :as sut]
            [d-core.core.authn.protocol :as p]))

(defn- missing-authenticator
  [type-key]
  (reify p/Authenticator
    (authenticate [_ _ _]
      (throw (ex-info "missing" {:type type-key})))
    (verify-token [_ _ _] nil)
    (challenge [_ _ _] {:status 401})))

(defn- success-authenticator
  [principal]
  (reify p/Authenticator
    (authenticate [_ _ _] {:principal principal :token "ok"})
    (verify-token [_ _ _] principal)
    (challenge [_ _ _] {:status 401})))

(deftest chain-falls-through-missing-token
  (testing "default continue-on allows moving to next authenticator"
    (let [chain (sut/->ChainAuthenticator
                 [{:authenticator (missing-authenticator :d-core.core.authn.jwt/missing-token)
                   :continue-on #{:d-core.core.authn.jwt/missing-token
                                  :d-core.core.authn.api-key/missing-api-key}}
                  {:authenticator (success-authenticator {:subject "ak-1"})
                   :continue-on #{:d-core.core.authn.jwt/missing-token
                                  :d-core.core.authn.api-key/missing-api-key}}]
                 nil)
          result (p/authenticate chain {:headers {}} {})]
      (is (= "ak-1" (get-in result [:principal :subject]))))))

(deftest chain-stops-on-non-continuable-error
  (testing "invalid credentials should fail fast"
    (let [bad (reify p/Authenticator
                (authenticate [_ _ _]
                  (throw (ex-info "invalid" {:type :d-core.core.authn.jwt/invalid-signature})))
                (verify-token [_ _ _] nil)
                (challenge [_ _ _] {:status 401}))
          chain (sut/->ChainAuthenticator
                 [{:authenticator bad :continue-on #{:d-core.core.authn.jwt/missing-token}}
                  {:authenticator (success-authenticator {:subject "unused"}) :continue-on #{}}]
                 nil)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"invalid"
           (p/authenticate chain {:headers {}} {}))))))
