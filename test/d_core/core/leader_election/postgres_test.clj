(ns d-core.core.leader-election.postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.postgres :as postgres]
            [d-core.core.leader-election.protocol :as p]
            [integrant.core :as ig]))

(def ^:private fixed-now-ms
  1700000000000)

(defn- make-component
  []
  (postgres/->PostgresLeaderElection :datasource
                                     "node-1"
                                     "\"dcore_leader_elections\""
                                     15000
                                     (constantly fixed-now-ms)))

(deftest table-name-validation
  (testing "table names are quoted and validated conservatively"
    (is (= "\"dcore_leader_elections\"" (postgres/normalize-table-name nil)))
    (is (= "\"custom_table\"" (postgres/normalize-table-name "custom_table")))
    (is (= "\"custom_table\"" (postgres/normalize-table-name :custom_table)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Unsafe identifier"
         (postgres/normalize-table-name "bad-name")))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Unsafe identifier"
         (postgres/normalize-table-name "schema.table")))))

(deftest acquire-contracts
  (testing "acquire returns acquired result shape"
      (let [component (make-component)
          calls (atom [])]
      (with-redefs [common/generate-token (fn [] "token-1")
                    d-core.core.leader-election.postgres/try-acquire-row! (fn [datasource table-ident election-id owner-id token now-ms lease-ms]
                                                                            (swap! calls conj {:datasource datasource
                                                                                               :table-ident table-ident
                                                                                               :election-id election-id
                                                                                               :owner-id owner-id
                                                                                               :token token
                                                                                               :now-ms now-ms
                                                                                               :lease-ms lease-ms})
                                                                            {:owner_id owner-id
                                                                             :fencing 7
                                                                             :token token})]
        (is (= {:ok true
                :status :acquired
                :backend :postgres
                :election-id "orders"
                :owner-id "node-1"
                :fencing 7
                :remaining-ttl-ms 15000
                :token "token-1"}
               (p/acquire! component :orders nil)))
        (is (= [{:datasource :datasource
                 :table-ident "\"dcore_leader_elections\""
                 :election-id "orders"
                 :owner-id "node-1"
                 :token "token-1"
                 :now-ms fixed-now-ms
                 :lease-ms 15000}]
               @calls)))))

  (testing "acquire returns busy when another leader is active"
    (let [component (make-component)]
      (with-redefs [common/generate-token (fn [] "token-1")
                    d-core.core.leader-election.postgres/try-acquire-row! (fn [& _] nil)
                    d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _ _]
                                                                                {:owner_id "node-2"
                                                                                 :fencing 9
                                                                                 :expires_at_ms (+ fixed-now-ms 1200)})]
        (is (= {:ok true
                :status :busy
                :backend :postgres
                :election-id "orders"
                :owner-id "node-2"
                :fencing 9
                :remaining-ttl-ms 1200}
               (p/acquire! component "orders" {:lease-ms 2500})))))))

(deftest renew-contracts
  (testing "renew returns renewed result shape"
    (let [component (make-component)
          calls (atom [])]
      (with-redefs [d-core.core.leader-election.postgres/try-renew-row! (fn [datasource table-ident election-id token now-ms lease-ms]
                                                                          (swap! calls conj {:datasource datasource
                                                                                             :table-ident table-ident
                                                                                             :election-id election-id
                                                                                             :token token
                                                                                             :now-ms now-ms
                                                                                             :lease-ms lease-ms})
                                                                          {:owner_id "node-1"
                                                                           :fencing 7
                                                                           :token token})]
        (is (= {:ok true
                :status :renewed
                :backend :postgres
                :election-id "orders"
                :owner-id "node-1"
                :fencing 7
                :remaining-ttl-ms 9000
                :token "token-1"}
               (p/renew! component :orders "token-1" {:lease-ms 9000})))
        (is (= [{:datasource :datasource
                 :table-ident "\"dcore_leader_elections\""
                 :election-id "orders"
                 :token "token-1"
                 :now-ms fixed-now-ms
                 :lease-ms 9000}]
               @calls)))))

  (testing "renew returns lost when token does not match"
    (let [component (make-component)]
      (with-redefs [d-core.core.leader-election.postgres/try-renew-row! (fn [& _] nil)
                    d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _ _]
                                                                                {:owner_id "node-2"
                                                                                 :fencing 11
                                                                                 :expires_at_ms (+ fixed-now-ms 2500)})]
        (is (= {:ok true
                :status :lost
                :backend :postgres
                :election-id "orders"
                :owner-id "node-2"
                :fencing 11
                :remaining-ttl-ms 2500}
               (p/renew! component "orders" "stale-token" nil)))))))

(deftest resign-and-status-contracts
  (testing "resign returns released and not-owner statuses"
    (let [component (make-component)]
      (with-redefs [d-core.core.leader-election.postgres/try-resign-row! (fn [_ _ _ token _]
                                                                            (when (= token "token-1")
                                                                              {:fencing 7}))
                    d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _ _]
                                                                                {:owner_id "node-2"
                                                                                 :fencing 8
                                                                                 :expires_at_ms (+ fixed-now-ms 2000)})]
        (is (= {:ok true
                :status :released
                :backend :postgres
                :election-id "orders"
                :owner-id "node-1"
                :fencing 7}
               (p/resign! component :orders "token-1" nil)))
        (is (= {:ok true
                :status :not-owner
                :backend :postgres
                :election-id "orders"
                :owner-id "node-2"
                :fencing 8
                :remaining-ttl-ms 2000}
               (p/resign! component :orders "wrong-token" nil))))))

  (testing "status returns held and vacant without token"
    (let [component (make-component)]
      (with-redefs [d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _ _]
                                                                                {:owner_id "node-1"
                                                                                 :fencing 7
                                                                                 :expires_at_ms (+ fixed-now-ms 3000)})]
        (let [result (p/status component :orders nil)]
          (is (= {:ok true
                  :status :held
                  :backend :postgres
                  :election-id "orders"
                  :owner-id "node-1"
                  :fencing 7
                  :remaining-ttl-ms 3000}
                 result))
          (is (false? (contains? result :token))))))

    (let [component (make-component)]
      (with-redefs [d-core.core.leader-election.postgres/select-active-holder (fn [& _] nil)]
        (is (= {:ok true
                :status :vacant
                :backend :postgres
                :election-id "orders"}
               (p/status component :orders nil)))))))

(deftest init-key-defaults-and-validation
  (testing "init-key applies defaults without bootstrapping by default"
    (let [bootstrap-called? (atom false)
          component (with-redefs [d-core.core.leader-election.postgres/bootstrap-schema! (fn [& _]
                                                                                            (reset! bootstrap-called? true)
                                                                                            {:ok true})]
                      (ig/init-key :d-core.core.leader-election.postgres/postgres
                                   {:postgres-client {:datasource :datasource}}))]
      (is (= "\"dcore_leader_elections\"" (:table-ident component)))
      (is (= 15000 (:default-lease-ms component)))
      (is (string? (:owner-id component)))
      (is (false? @bootstrap-called?))))

  (testing "init-key bootstraps schema when enabled"
    (let [calls (atom [])]
      (with-redefs [d-core.core.leader-election.postgres/bootstrap-schema! (fn [datasource table-ident]
                                                                              (swap! calls conj {:datasource datasource
                                                                                                 :table-ident table-ident})
                                                                              {:ok true})]
        (ig/init-key :d-core.core.leader-election.postgres/postgres
                     {:postgres-client {:datasource :datasource}
                      :bootstrap-schema? true
                      :table-name "custom_table"}))
      (is (= [{:datasource :datasource
               :table-ident "\"custom_table\""}]
             @calls))))

  (testing "init-key validates required client"
    (is (thrown? clojure.lang.ExceptionInfo
                 (ig/init-key :d-core.core.leader-election.postgres/postgres {})))))
