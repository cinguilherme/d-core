(ns d-core.core.api-keys.postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.api-keys.postgres :as sut]
            [d-core.core.api-keys.protocol :as p])
  (:import (java.sql Timestamp)
           (java.time Instant)))

(deftest create-key-generates-token-and-sanitized-metadata
  (testing "create-key! returns one-time token and normalized api-key map"
    (let [store (sut/->PostgresApiKeyStore :ds "pepper" nil)
          now-ts (Timestamp/from (Instant/parse "2026-03-01T00:00:00Z"))]
      (with-redefs [d-core.core.api-keys.postgres/generate-prefix (constantly "pref123456789012")
                    d-core.core.api-keys.postgres/generate-secret (constantly "secret-abc")
                    d-core.core.api-keys.postgres/query-one (fn [_ _]
                                                              {:api_key_id "8e0a7d31-56c8-4bd1-bcd9-2ff6fbf0d995"
                                                               :name "integration"
                                                               :tenant_id "tenant-1"
                                                               :scopes_json "[\"messages:read\",\"messages:write\"]"
                                                               :key_prefix "pref123456789012"
                                                               :status "active"
                                                               :expires_at nil
                                                               :last_used_at nil
                                                               :limits_json "{\"rate-limit\":{\"limit\":10,\"window-ms\":60000}}"
                                                               :metadata_json "{\"env\":\"dev\"}"
                                                               :created_at now-ts
                                                               :updated_at now-ts
                                                               :revoked_at nil})]
        (let [result (p/create-key! store {:name "integration"
                                           :tenant-id "tenant-1"
                                           :scopes #{"messages:read" "messages:write"}
                                           :limits {:rate-limit {:limit 10 :window-ms 60000}}
                                           :metadata {:env "dev"}}
                                    {})]
          (is (= "dck_pref123456789012.secret-abc" (:token result)))
          (is (= "tenant-1" (get-in result [:api-key :tenant-id])))
          (is (= #{"messages:read" "messages:write"} (get-in result [:api-key :scopes])))
          (is (= :active (get-in result [:api-key :status])))
          (is (= {:rate-limit {:limit 10 :window-ms 60000}} (get-in result [:api-key :limits]))))))))

(deftest authenticate-key-validates-format-and-hash
  (testing "authenticate-key returns nil for invalid format and map for valid keys"
    (let [store (sut/->PostgresApiKeyStore :ds "pepper" nil)
          future-ts (Timestamp/from (.plusSeconds (Instant/now) 600))
          hash-value (#'sut/secret->hash "pepper" "secret-abc")
          updates (atom [])]
      (is (nil? (p/authenticate-key store "bad-key" {})))
      (with-redefs [d-core.core.api-keys.postgres/query-one (fn [_ _]
                                                              {:api_key_id "8e0a7d31-56c8-4bd1-bcd9-2ff6fbf0d995"
                                                               :name "integration"
                                                               :tenant_id "tenant-1"
                                                               :scopes_json "[\"messages:read\"]"
                                                               :key_prefix "pref123456789012"
                                                               :key_hash hash-value
                                                               :status "active"
                                                               :expires_at future-ts
                                                               :last_used_at nil
                                                               :limits_json "{\"rate-limit\":{\"limit\":2,\"window-ms\":1000}}"
                                                               :metadata_json "{}"
                                                               :created_at future-ts
                                                               :updated_at future-ts
                                                               :revoked_at nil})
                    next.jdbc/execute! (fn [_ sqlvec & _]
                                         (swap! updates conj sqlvec)
                                         [{:next.jdbc/update-count 1}])]
        (let [result (p/authenticate-key store "dck_pref123456789012.secret-abc" {})]
          (is (= "8e0a7d31-56c8-4bd1-bcd9-2ff6fbf0d995" (:api-key-id result)))
          (is (= #{"messages:read"} (:scopes result)))
          (is (= {:rate-limit {:limit 2 :window-ms 1000}} (:limits result)))
          (is (= 1 (count @updates))))))))

(deftest consume-rate-limit-returns-expected-window-shape
  (testing "consume-rate-limit! computes remaining/retry fields"
    (let [store (sut/->PostgresApiKeyStore :ds "pepper" nil)
          window-start (Timestamp/from (Instant/ofEpochMilli 1000))]
      (with-redefs [d-core.core.api-keys.postgres/query-one (fn [_ _]
                                                              {:window_start window-start
                                                               :request_count 5})
                    d-core.core.api-keys.postgres/now-ms (constantly 1500)]
        (let [result (p/consume-rate-limit! store "8e0a7d31-56c8-4bd1-bcd9-2ff6fbf0d995"
                                            {:limit 3 :window-ms 1000})]
          (is (false? (:allowed? result)))
          (is (= 0 (:remaining result)))
          (is (= 2000 (:reset-at result)))
          (is (= 500 (:retry-after-ms result))))))))
