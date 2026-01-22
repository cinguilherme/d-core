(ns d-core.integration.auth-test
  (:require [clojure.test :refer [deftest is testing]]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [d-core.core.auth.http :as auth-http]
            [d-core.core.auth.token-client :as token-client]
            [d-core.core.authn.jwt :as jwt]
            [d-core.core.authn.protocol :as authn]
            [d-core.core.authz.scope :as scope]))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_AUTH"))))

(defn- keycloak-url
  []
  (or (System/getenv "DCORE_KEYCLOAK_URL")
      "http://localhost:8080"))

(defn- realm
  []
  (or (System/getenv "DCORE_KEYCLOAK_REALM")
      "d-core"))

(defn- issuer
  []
  (str (keycloak-url) "/realms/" (realm)))

(defn- token-url
  []
  (str (issuer) "/protocol/openid-connect/token"))

(defn- jwks-url
  []
  (str (issuer) "/protocol/openid-connect/certs"))

(defn- request-token
  [params]
  (let [resp (http/post (token-url)
                        {:form-params params
                         :as :text
                         :throw-exceptions false})
        body (json/parse-string (:body resp) true)]
    (when-not (= 200 (:status resp))
      (throw (ex-info "Token request failed"
                      {:status (:status resp)
                       :body body})))
    body))

(defn- user-token
  []
  (:access_token
   (request-token {:grant_type "password"
                   :client_id "d-core-api"
                   :username "alice"
                   :password "alice"})))

(defn- service-token
  []
  (let [client (token-client/make-client
                 {:token-url (token-url)
                  :client-id "d-core-service"
                  :client-secret "d-core-secret"})]
    (:access-token (token-client/client-credentials client {}))))

(defn- authenticator
  []
  (jwt/->JwtAuthenticator (issuer) nil nil (jwks-url) nil nil
                          "tenant_id" "dcore_scope" nil nil (atom nil)))

(deftest keycloak-jwt-authentication
  (testing "JWT authenticator verifies and normalizes user token"
    (if-not (integration-enabled?)
      (is true "Skipping Keycloak integration test; set DCORE_INTEGRATION=1")
      (let [token (user-token)
            result (authn/authenticate (authenticator)
                                       {:headers {"authorization" (str "Bearer " token)}}
                                       {})
            principal (:principal result)]
        (is (= "tenant-1" (:tenant-id principal)))
        (is (= #{"messages:read" "messages:write"} (:scopes principal)))
        (is (= "d-core-api" (:client-id principal)))
        (is (= (issuer) (:issuer principal)))))))

(deftest keycloak-service-token
  (testing "Client credentials flow yields verifiable token"
    (if-not (integration-enabled?)
      (is true "Skipping Keycloak integration test; set DCORE_INTEGRATION=1")
      (let [token (service-token)
            principal (authn/verify-token (authenticator) token {})]
        (is (= "tenant-1" (:tenant-id principal)))
        (is (= #{"messages:read" "messages:write"} (:scopes principal)))
        (is (= "d-core-service" (:client-id principal)))))))

(deftest keycloak-http-middleware
  (testing "Authentication + authorization middleware works with real tokens"
    (if-not (integration-enabled?)
      (is true "Skipping Keycloak integration test; set DCORE_INTEGRATION=1")
      (let [token (user-token)
            authorizer (scope/->ScopeAuthorizer nil)
            handler (fn [_] {:status 200})
            app (-> handler
                    (auth-http/wrap-authorization authorizer {})
                    (auth-http/wrap-authentication (authenticator) {}))
            allow (app {:headers {"authorization" (str "Bearer " token)}
                        :auth/require {:tenant "tenant-1"
                                       :scopes #{"messages:read"}}})
            deny (app {:headers {"authorization" (str "Bearer " token)}
                       :auth/require {:tenant "tenant-1"
                                      :scopes #{"admin"}}})]
        (is (= 200 (:status allow)))
        (is (= 403 (:status deny)))))))
