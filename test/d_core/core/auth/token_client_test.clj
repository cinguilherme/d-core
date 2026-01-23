(ns d-core.core.auth.token-client-test
  (:require [clojure.test :refer [deftest is testing]]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [d-core.core.auth.token-client :as token-client]))

(deftest make-client-requires-token-url
  (testing "token-url is mandatory"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"token-url"
         (token-client/make-client {})))))

(deftest client-credentials-request
  (testing "Client credentials flow posts expected params"
    (let [captured (atom nil)
          client (token-client/make-client {:token-url "http://token"
                                            :client-id "client-id"
                                            :client-secret "client-secret"})]
      (with-redefs [http/post (fn [url opts]
                                (reset! captured {:url url :opts opts})
                                {:status 200
                                 :body (json/generate-string
                                        {:access_token "token"
                                         :refresh_token "refresh"
                                         :expires_in 60
                                         :token_type "Bearer"
                                         :scope "read"})})]
        (let [resp (token-client/client-credentials client
                                                    {:scope "read"
                                                     :audience "api"})]
          (is (= "token" (:access-token resp)))
          (is (= "http://token" (:url @captured)))
          (is (= {:grant_type "client_credentials"
                  :scope "read"
                  :audience "api"
                  :client_id "client-id"
                  :client_secret "client-secret"}
                 (get-in @captured [:opts :form-params]))))))))

(deftest token-exchange-request
  (testing "Token exchange includes required fields"
    (let [captured (atom nil)
          client (token-client/make-client {:token-url "http://token"
                                            :client-id "client-id"})]
      (with-redefs [http/post (fn [url opts]
                                (reset! captured {:url url :opts opts})
                                {:status 200
                                 :body (json/generate-string
                                        {:access_token "token"})})]
        (let [resp (token-client/token-exchange client
                                                {:subject-token "user-token"
                                                 :actor-token "service-token"
                                                 :audience "api"
                                                 :scope "read"})]
          (is (= "token" (:access-token resp)))
          (is (= "http://token" (:url @captured)))
          (is (= {:grant_type "urn:ietf:params:oauth:grant-type:token-exchange"
                  :subject_token "user-token"
                  :subject_token_type "urn:ietf:params:oauth:token-type:access_token"
                  :requested_token_type "urn:ietf:params:oauth:token-type:access_token"
                  :actor_token "service-token"
                  :audience "api"
                  :scope "read"
                  :client_id "client-id"}
                 (get-in @captured [:opts :form-params]))))))))

(deftest token-request-failure
  (testing "Non-2xx responses raise exception"
    (let [client (token-client/make-client {:token-url "http://token"})]
      (with-redefs [http/post (fn [_ _]
                                {:status 400
                                 :body (json/generate-string
                                        {:error "invalid_request"})})]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Token request failed"
             (token-client/client-credentials client {})))))))
