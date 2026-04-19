(ns d-core.core.leader-election.postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.observability :as obs]
            [d-core.core.leader-election.postgres :as postgres]
            [d-core.core.leader-election.protocol :as p]
            [d-core.helpers.logger :as h-logger]
            [d-core.helpers.metrics :as h-metrics]
            [integrant.core :as ig]))

(defn- make-component
  ([] (make-component nil))
  ([observability]
  (postgres/->PostgresLeaderElection :datasource
                                     "node-1"
                                     "\"dcore_leader_elections\""
                                     15000
                                     (constantly 1700000000000)
                                     observability)))

(defn- make-observed-component
  []
  (let [{:keys [logger logs]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (h-metrics/make-test-metrics)]
    {:component (make-component (obs/make-context logger metrics))
     :logs logs
     :metric-calls calls
     :logger logger
     :metrics metrics}))

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
                    d-core.core.leader-election.postgres/try-acquire-row! (fn [datasource table-ident election-id owner-id token lease-ms]
                                                                            (swap! calls conj {:datasource datasource
                                                                                               :table-ident table-ident
                                                                                               :election-id election-id
                                                                                               :owner-id owner-id
                                                                                               :token token
                                                                                               :lease-ms lease-ms})
                                                                            {:owner_id owner-id
                                                                             :fencing 7
                                                                             :token token
                                                                             :remaining_ttl_ms 15000})]
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
                 :lease-ms 15000}]
               @calls)))))

  (testing "acquire returns busy when another leader is active"
    (let [component (make-component)]
      (with-redefs [common/generate-token (fn [] "token-1")
                    d-core.core.leader-election.postgres/try-acquire-row! (fn [& _] nil)
                    d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _]
                                                                                {:owner_id "node-2"
                                                                                 :fencing 9
                                                                                 :remaining_ttl_ms 1200})]
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
      (with-redefs [d-core.core.leader-election.postgres/try-renew-row! (fn [datasource table-ident election-id token lease-ms]
                                                                          (swap! calls conj {:datasource datasource
                                                                                             :table-ident table-ident
                                                                                             :election-id election-id
                                                                                             :token token
                                                                                             :lease-ms lease-ms})
                                                                          {:owner_id "node-1"
                                                                           :fencing 7
                                                                           :token token
                                                                           :remaining_ttl_ms 9000})]
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
                 :lease-ms 9000}]
               @calls)))))

  (testing "renew returns lost when token does not match"
    (let [component (make-component)]
      (with-redefs [d-core.core.leader-election.postgres/try-renew-row! (fn [& _] nil)
                    d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _]
                                                                                {:owner_id "node-2"
                                                                                 :fencing 11
                                                                                 :remaining_ttl_ms 2500})]
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
      (with-redefs [d-core.core.leader-election.postgres/try-resign-row! (fn [_ _ _ token]
                                                                            (when (= token "token-1")
                                                                              {:fencing 7}))
                    d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _]
                                                                                {:owner_id "node-2"
                                                                                 :fencing 8
                                                                                 :remaining_ttl_ms 2000})]
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
      (with-redefs [d-core.core.leader-election.postgres/select-active-holder (fn [_ _ _]
                                                                                {:owner_id "node-1"
                                                                                 :fencing 7
                                                                                 :remaining_ttl_ms 3000})]
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

  (testing "init-key accepts logger and metrics"
    (let [{:keys [logger]} (h-logger/make-test-logger)
          {:keys [metrics]} (h-metrics/make-test-metrics)
          component (ig/init-key :d-core.core.leader-election.postgres/postgres
                                 {:postgres-client {:datasource :datasource}
                                  :logger logger
                                  :metrics metrics})]
      (is (= logger (get-in component [:observability :logger])))
      (is (= metrics (get-in component [:observability :metrics])))))

  (testing "init-key validates required client"
    (is (thrown? clojure.lang.ExceptionInfo
                 (ig/init-key :d-core.core.leader-election.postgres/postgres {}))))

  (testing "init-key validates metrics dependency"
    (let [ex (try
               (ig/init-key :d-core.core.leader-election.postgres/postgres
                            {:postgres-client {:datasource :datasource}
                             :metrics {}})
               nil
               (catch clojure.lang.ExceptionInfo ex
                 ex))]
      (is (instance? clojure.lang.ExceptionInfo ex))
      (is (= "d-core.core.metrics.protocol/MetricsProtocol"
             (:expected (ex-data ex))))))

  (testing "init-key validates default-lease-ms with typed errors"
    (doseq [invalid [nil "10" :x 1.5]]
      (let [ex (try
                 (ig/init-key :d-core.core.leader-election.postgres/postgres
                              {:postgres-client {:datasource :datasource}
                               :default-lease-ms invalid})
                 nil
                 (catch clojure.lang.ExceptionInfo ex
                   ex))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (= ::common/invalid-field (:type (ex-data ex))))
        (is (= :default-lease-ms (:field (ex-data ex))))
        (is (= invalid (:value (ex-data ex))))))))

(deftest observability-contracts
  (testing "lifecycle outcomes emit consistent logs and metrics"
    (let [{:keys [component logs metric-calls]} (make-observed-component)]
      (with-redefs [common/generate-token (fn [] "token-1")
                    d-core.core.leader-election.postgres/try-acquire-row! (fn [_ _ election-id _ token _]
                                                                            (when (= election-id "orders-acquired")
                                                                              {:owner_id "node-1"
                                                                               :fencing 7
                                                                               :token token
                                                                               :remaining_ttl_ms 15000}))
                    d-core.core.leader-election.postgres/try-renew-row! (fn [_ _ _ token _]
                                                                          (when (= token "token-1")
                                                                            {:owner_id "node-1"
                                                                             :fencing 7
                                                                             :token token
                                                                             :remaining_ttl_ms 9000}))
                    d-core.core.leader-election.postgres/try-resign-row! (fn [_ _ _ token]
                                                                           (when (= token "token-1")
                                                                             {:fencing 7}))
                    d-core.core.leader-election.postgres/select-active-holder (fn [_ _ election-id]
                                                                                (case election-id
                                                                                  "orders-busy" {:owner_id "node-2"
                                                                                                 :fencing 9
                                                                                                 :remaining_ttl_ms 1200}
                                                                                  "orders-lost" {:owner_id "node-2"
                                                                                                 :fencing 11
                                                                                                 :remaining_ttl_ms 2500}
                                                                                  "orders-not-owner" {:owner_id "node-2"
                                                                                                      :fencing 8
                                                                                                      :remaining_ttl_ms 2000}
                                                                                  "orders-held" {:owner_id "node-1"
                                                                                                 :fencing 7
                                                                                                 :remaining_ttl_ms 3000}
                                                                                  nil))]
        (is (= :acquired (:status (p/acquire! component :orders-acquired nil))))
        (is (= :busy (:status (p/acquire! component :orders-busy nil))))
        (is (= :renewed (:status (p/renew! component :orders-renewed "token-1" {:lease-ms 9000}))))
        (is (= :lost (:status (p/renew! component :orders-lost "wrong-token" nil))))
        (is (= :released (:status (p/resign! component :orders-released "token-1" nil))))
        (is (= :not-owner (:status (p/resign! component :orders-not-owner "wrong-token" nil))))
        (is (= :held (:status (p/status component :orders-held nil))))
        (is (= :vacant (:status (p/status component :orders-vacant nil))))
        (is (= [["info" "acquired"]
                ["debug" "busy"]
                ["debug" "renewed"]
                ["warn" "lost"]
                ["info" "released"]
                ["debug" "not-owner"]]
               (mapv (fn [{:keys [level data]}]
                       [(name level) (name (:status data))])
                     @logs)))
        (is (= #{"acquired" "busy" "renewed" "lost" "released" "not-owner" "held" "vacant"}
               (set (map last
                         (map :labels
                              (map :metric
                                   (h-metrics/find-calls metric-calls :inc! :leader_election_requests_total)))))))
        (is (= 8 (count (h-metrics/find-calls metric-calls :observe! :leader_election_request_duration_seconds))))
        (is (not-any? #(contains? #{:held :vacant} (get-in % [:data :status])) @logs)))))

  (testing "backend exceptions emit one error metric and log then rethrow"
    (let [{:keys [component logs metric-calls]} (make-observed-component)
          boom (RuntimeException. "boom")]
      (with-redefs [common/generate-token (fn [] "token-1")
                    d-core.core.leader-election.postgres/try-acquire-row! (fn [& _]
                                                                            (throw boom))]
        (is (thrown-with-msg? RuntimeException #"boom"
                              (p/acquire! component :orders nil)))
        (is (= ["postgres" "acquire" "exception"]
               (get-in (first (h-metrics/find-calls metric-calls :inc! :leader_election_errors_total))
                       [:metric :labels])))
        (is (= ["postgres" "acquire"]
               (get-in (first (h-metrics/find-calls metric-calls :observe! :leader_election_request_duration_seconds))
                       [:metric :labels])))
        (is (= [{:level :error
                 :event ::obs/operation-failed
                 :data {:backend :postgres
                        :op :acquire
                        :type :exception
                        :error "boom"
                        :election-id "orders"}}]
               @logs))))))
