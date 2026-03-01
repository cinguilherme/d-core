(ns d-core.core.authn.api-key-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.api-keys.protocol :as api-keys]
            [d-core.core.authn.api-key :as sut]
            [d-core.core.authn.protocol :as p]))

(defn- fake-store
  [authenticate-fn]
  (reify api-keys/ApiKeyStore
    (ensure-schema! [_ _] {:ok true})
    (create-key! [_ _ _] nil)
    (get-key [_ _ _] nil)
    (list-keys [_ _ _] [])
    (revoke-key! [_ _ _] {:revoked? false})
    (rotate-key! [_ _ _] nil)
    (authenticate-key [_ token opts] (authenticate-fn token opts))
    (consume-rate-limit! [_ _ _] {:allowed? true :remaining 1 :reset-at nil :retry-after-ms nil})))

(deftest authenticate-success
  (testing "extracts x-api-key and builds principal"
    (let [store (fake-store (fn [token _]
                              (when (= token "dck_x.y")
                                {:api-key-id "ak-1"
                                 :tenant-id "tenant-1"
                                 :scopes #{"messages:read"}
                                 :limits {:rate-limit {:limit 10 :window-ms 60000}}
                                 :metadata {:env "dev"}})))
          authn (sut/->ApiKeyAuthenticator store nil nil)
          result (p/authenticate authn {:headers {"x-api-key" "dck_x.y"}} {})]
      (is (= "dck_x.y" (:token result)))
      (is (= "ak-1" (get-in result [:principal :api-key/id])))
      (is (= :api-key (get-in result [:principal :auth-type])))
      (is (= #{"messages:read"} (get-in result [:principal :scopes]))))))

(deftest authenticate-authorization-header
  (testing "supports Authorization: ApiKey"
    (let [store (fake-store (fn [token _]
                              (when (= token "dck_x.y")
                                {:api-key-id "ak-1"
                                 :tenant-id "tenant-1"
                                 :scopes #{"messages:read"}})))
          authn (sut/->ApiKeyAuthenticator store nil nil)
          result (p/authenticate authn {:headers {"authorization" "ApiKey dck_x.y"}} {})]
      (is (= "ak-1" (get-in result [:principal :api-key/id]))))))

(deftest authenticate-missing-key
  (testing "throws when key is missing"
    (let [store (fake-store (fn [_ _] nil))
          authn (sut/->ApiKeyAuthenticator store nil nil)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Missing API key"
           (p/authenticate authn {:headers {}} {}))))))

(deftest authenticate-invalid-key
  (testing "throws when key cannot be authenticated"
    (let [store (fake-store (fn [_ _] nil))
          authn (sut/->ApiKeyAuthenticator store nil nil)]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid API key"
           (p/authenticate authn {:headers {"x-api-key" "bad"}} {}))))))

(deftest challenge-response
  (testing "returns ApiKey challenge header"
    (let [store (fake-store (fn [_ _] nil))
          authn (sut/->ApiKeyAuthenticator store "example" nil)
          resp (p/challenge authn {:headers {"host" "localhost"}} {})]
      (is (= 401 (:status resp)))
      (is (= "ApiKey realm=\"example\"" (get-in resp [:headers "WWW-Authenticate"])))))) 
