(ns d-core.core.authn.jwt-test
  (:require [clojure.test :refer [deftest is testing]]
            [cheshire.core :as json]
            [d-core.core.authn.jwt :as jwt]
            [d-core.core.authn.protocol :as p])
  (:import (java.math BigInteger)
           (java.security KeyPairGenerator Signature)
           (java.security.interfaces RSAPublicKey)
           (java.util Arrays Base64)))

(defn- b64url-encode
  ^String [^bytes data]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) data))

(defn- bigint->bytes
  ^bytes [^BigInteger bi]
  (let [bytes (.toByteArray bi)]
    (if (and (> (alength bytes) 1) (zero? (aget bytes 0)))
      (Arrays/copyOfRange bytes 1 (alength bytes))
      bytes)))

(defn- rsa-key-pair []
  (let [gen (KeyPairGenerator/getInstance "RSA")]
    (.initialize gen 2048)
    (.generateKeyPair gen)))

(defn- jwk-from-public
  [^RSAPublicKey public-key kid]
  {:kty "RSA"
   :kid kid
   :n (b64url-encode (bigint->bytes (.getModulus public-key)))
   :e (b64url-encode (bigint->bytes (.getPublicExponent public-key)))})

(defn- sign-jwt
  [private-key header claims]
  (let [header-json (json/generate-string header)
        claims-json (json/generate-string claims)
        header-b64 (b64url-encode (.getBytes header-json "UTF-8"))
        claims-b64 (b64url-encode (.getBytes claims-json "UTF-8"))
        signing-input (str header-b64 "." claims-b64)
        sig (Signature/getInstance "SHA256withRSA")]
    (.initSign sig private-key)
    (.update sig (.getBytes signing-input "UTF-8"))
    (let [signature (.sign sig)]
      (str signing-input "." (b64url-encode signature)))))

(defn- make-authenticator
  [{:keys [issuer aud jwks]}]
  (jwt/->JwtAuthenticator issuer aud jwks nil nil nil nil nil nil nil (atom nil)))

(deftest jwt-authenticate-valid-token
  (testing "Authenticates valid JWT and normalizes principal"
    (let [kp (rsa-key-pair)
          kid "test-kid"
          jwks {:keys [(jwk-from-public (.getPublic kp) kid)]}
          now-sec (long (/ (System/currentTimeMillis) 1000))
          claims {:sub "user-1"
                  :iss "https://issuer.example.com/realm/dev"
                  :aud "d-core-api"
                  :exp (+ now-sec 60)
                  :scope "messages:read messages:write"
                  :tenant_id "tenant-1"
                  :azp "service-a"}
          token (sign-jwt (.getPrivate kp)
                          {:alg "RS256" :typ "JWT" :kid kid}
                          claims)
          authenticator (make-authenticator {:issuer "https://issuer.example.com/realm/dev"
                                             :aud "d-core-api"
                                             :jwks jwks})
          result (p/authenticate authenticator
                                 {:headers {"authorization" (str "Bearer " token)}}
                                 {})
          principal (:principal result)]
      (is (= token (:token result)))
      (is (= "user-1" (:subject principal)))
      (is (= "tenant-1" (:tenant-id principal)))
      (is (= #{"messages:read" "messages:write"} (:scopes principal)))
      (is (= "service-a" (:client-id principal)))
      (is (= "d-core-api" (:aud principal))))))

(deftest jwt-authenticate-missing-token
  (testing "Missing bearer token raises error"
    (let [authenticator (make-authenticator {:issuer "https://issuer"
                                             :aud "d-core-api"
                                             :jwks {:keys []}})]
      (try
        (p/authenticate authenticator {:headers {}} {})
        (is false "Expected exception")
        (catch clojure.lang.ExceptionInfo ex
          (is (= ::jwt/missing-token (:type (ex-data ex)))))))))

(deftest jwt-audience-mismatch
  (testing "Audience mismatch is rejected"
    (let [kp (rsa-key-pair)
          kid "test-kid"
          jwks {:keys [(jwk-from-public (.getPublic kp) kid)]}
          now-sec (long (/ (System/currentTimeMillis) 1000))
          claims {:sub "user-1"
                  :iss "https://issuer"
                  :aud "other-aud"
                  :exp (+ now-sec 60)}
          token (sign-jwt (.getPrivate kp)
                          {:alg "RS256" :kid kid}
                          claims)
          authenticator (make-authenticator {:issuer "https://issuer"
                                             :aud "expected-aud"
                                             :jwks jwks})]
      (try
        (p/verify-token authenticator token {})
        (is false "Expected exception")
        (catch clojure.lang.ExceptionInfo ex
          (is (= ::jwt/audience-mismatch (:type (ex-data ex)))))))))

(deftest jwt-expired-token
  (testing "Expired token is rejected"
    (let [kp (rsa-key-pair)
          kid "test-kid"
          jwks {:keys [(jwk-from-public (.getPublic kp) kid)]}
          now-sec (long (/ (System/currentTimeMillis) 1000))
          claims {:sub "user-1"
                  :iss "https://issuer"
                  :aud "d-core-api"
                  :exp (- now-sec 120)}
          token (sign-jwt (.getPrivate kp)
                          {:alg "RS256" :kid kid}
                          claims)
          authenticator (make-authenticator {:issuer "https://issuer"
                                             :aud "d-core-api"
                                             :jwks jwks})]
      (try
        (p/verify-token authenticator token {})
        (is false "Expected exception")
        (catch clojure.lang.ExceptionInfo ex
          (is (= ::jwt/token-expired (:type (ex-data ex)))))))))

(deftest jwks-cache-refresh-is-single-flight
  (testing "Only one fetch occurs when cache is stale and requests race"
    (let [counter (atom 0)
          state (atom nil)
          start (promise)
          jwks {:keys [{:kty "RSA" :kid "kid" :n "n" :e "e"}]}
          opts {:jwks-uri "http://jwks"
                :jwks-cache-ttl-ms 60000
                :http-opts {}
                :state state}]
      (with-redefs [jwt/fetch-jwks (fn [_ _]
                                    (swap! counter inc)
                                    (Thread/sleep 100)
                                    jwks)]
        (let [futs (doall (repeatedly 8 #(future @start (#'jwt/get-jwks opts))))]
          (deliver start true)
          (doseq [f futs]
            (is (= jwks @f)))
          (is (= 1 @counter)))))))

(deftest jwks-cache-uses-fresh-value
  (testing "Fresh cache avoids fetch"
    (let [now (System/currentTimeMillis)
          jwks {:keys [{:kty "RSA" :kid "kid" :n "n" :e "e"}]}
          state (atom {:jwks jwks :fetched-at now})
          opts {:jwks-uri "http://jwks"
                :jwks-cache-ttl-ms 60000
                :http-opts {}
                :state state}]
      (with-redefs [jwt/fetch-jwks (fn [_ _]
                                    (throw (ex-info "should not fetch" {})))]
        (is (= jwks (#'jwt/get-jwks opts)))))))
