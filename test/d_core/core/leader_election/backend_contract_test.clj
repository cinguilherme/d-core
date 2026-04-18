(ns d-core.core.leader-election.backend-contract-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.protocol :as p]
            [d-core.core.leader-election.redis-common :as redis-common]
            [d-core.core.leader-election.redis :as redis]
            [d-core.core.leader-election.valkey :as valkey]
            [integrant.core :as ig]))

(def ^:private fixed-now-ms
  1700000000000)

(def ^:private backend-cases
  [{:name :redis
    :backend :redis
    :make-component #(redis/->RedisLeaderElection :client "node-1" "dcore:leader-election:" 15000 (constantly fixed-now-ms))
    :acquire-var #'d-core.core.leader-election.redis/eval-acquire!
    :renew-var #'d-core.core.leader-election.redis/eval-renew!
    :resign-var #'d-core.core.leader-election.redis/eval-resign!
    :status-var #'d-core.core.leader-election.redis/eval-status
    :init-key :d-core.core.leader-election.redis/redis
    :client-key :redis-client}
   {:name :valkey
    :backend :valkey
    :make-component #(valkey/->ValkeyLeaderElection :client "node-1" "dcore:leader-election:" 15000 (constantly fixed-now-ms))
    :acquire-var #'d-core.core.leader-election.valkey/eval-acquire!
    :renew-var #'d-core.core.leader-election.valkey/eval-renew!
    :resign-var #'d-core.core.leader-election.valkey/eval-resign!
    :status-var #'d-core.core.leader-election.valkey/eval-status
    :init-key :d-core.core.leader-election.valkey/valkey
    :client-key :valkey-client}])

(deftest acquire-contracts
  (doseq [{:keys [name backend make-component acquire-var]} backend-cases]
    (testing (str name " acquire returns acquired result shape")
      (let [component (make-component)
            calls (atom [])]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [client lease-key fencing-key owner-id token now-ms lease-ms]
                                       (swap! calls conj {:client client
                                                          :lease-key lease-key
                                                          :fencing-key fencing-key
                                                          :owner-id owner-id
                                                          :token token
                                                          :now-ms now-ms
                                                          :lease-ms lease-ms})
                                       ["acquired" owner-id "7" token lease-ms])}
          (fn []
            (is (= {:ok true
                    :status :acquired
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7
                    :remaining-ttl-ms 15000
                    :token "token-1"}
                   (p/acquire! component :orders nil)))
            (is (= [{:client :client
                     :lease-key "dcore:leader-election:orders:lease"
                     :fencing-key "dcore:leader-election:orders:fencing"
                     :owner-id "node-1"
                     :token "token-1"
                     :now-ms fixed-now-ms
                     :lease-ms 15000}]
                   @calls))))))

    (testing (str name " acquire returns busy without leaking token")
      (let [component (make-component)]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [& _]
                                       ["busy" "node-2" "9" "" "1200"])}
          (fn []
            (let [result (p/acquire! component "orders" {:lease-ms 2500})]
              (is (= {:ok true
                      :status :busy
                      :backend backend
                      :election-id "orders"
                      :owner-id "node-2"
                      :fencing 9
                      :remaining-ttl-ms 1200}
                     result))
              (is (false? (contains? result :token))))))))))

(deftest renew-contracts
  (doseq [{:keys [name backend make-component renew-var]} backend-cases]
    (testing (str name " renew returns renewed result shape")
      (let [component (make-component)
            calls (atom [])]
        (with-redefs-fn {renew-var (fn [client lease-key token now-ms lease-ms]
                                     (swap! calls conj {:client client
                                                        :lease-key lease-key
                                                        :token token
                                                        :now-ms now-ms
                                                        :lease-ms lease-ms})
                                     ["renewed" "node-1" "7" token lease-ms])}
          (fn []
            (is (= {:ok true
                    :status :renewed
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7
                    :remaining-ttl-ms 9000
                    :token "token-1"}
                   (p/renew! component :orders "token-1" {:lease-ms 9000})))
            (is (= [{:client :client
                     :lease-key "dcore:leader-election:orders:lease"
                     :token "token-1"
                     :now-ms fixed-now-ms
                     :lease-ms 9000}]
                   @calls))))))

    (testing (str name " renew returns lost when token does not match")
      (let [component (make-component)]
        (with-redefs-fn {renew-var (fn [& _]
                                     ["lost" "node-2" "11" "2500"])}
          (fn []
            (let [result (p/renew! component "orders" "stale-token" nil)]
              (is (= {:ok true
                      :status :lost
                      :backend backend
                      :election-id "orders"
                      :owner-id "node-2"
                      :fencing 11
                      :remaining-ttl-ms 2500}
                     result))
              (is (false? (contains? result :token))))))))))

(deftest resign-and-status-contracts
  (doseq [{:keys [name backend make-component resign-var status-var]} backend-cases]
    (testing (str name " resign returns released and not-owner statuses")
      (let [component (make-component)]
        (with-redefs-fn {resign-var (fn [_client _lease-key token]
                                      (if (= token "token-1")
                                        ["released" "node-1" "7"]
                                        ["not-owner" "node-2" "8" "2000"]))}
          (fn []
            (is (= {:ok true
                    :status :released
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7}
                   (p/resign! component :orders "token-1" nil)))
            (is (= {:ok true
                    :status :not-owner
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-2"
                    :fencing 8
                    :remaining-ttl-ms 2000}
                   (p/resign! component :orders "wrong-token" nil)))))))

    (testing (str name " status returns held without token")
      (let [component (make-component)]
        (with-redefs-fn {status-var (fn [_client _lease-key]
                                      ["held" "node-1" "7" "3000"])}
          (fn []
            (let [result (p/status component :orders nil)]
              (is (= {:ok true
                      :status :held
                      :backend backend
                      :election-id "orders"
                      :owner-id "node-1"
                      :fencing 7
                      :remaining-ttl-ms 3000}
                     result))
              (is (false? (contains? result :token))))))))

    (testing (str name " status returns vacant")
      (let [component (make-component)]
        (with-redefs-fn {status-var (fn [_client _lease-key] ["vacant"])}
          (fn []
            (is (= {:ok true
                    :status :vacant
                    :backend backend
                    :election-id "orders"}
                   (p/status component :orders nil)))))))))

(deftest init-key-defaults-and-validation
  (doseq [{:keys [name init-key client-key]} backend-cases]
    (testing (str name " init-key applies defaults")
      (let [component (ig/init-key init-key {client-key :client})]
        (is (= redis-common/default-prefix (:prefix component)))
        (is (= common/default-lease-ms (:default-lease-ms component)))
        (is (string? (:owner-id component)))
        (is (re-find #":" (:owner-id component)))))

    (testing (str name " init-key validates required client")
      (is (thrown? clojure.lang.ExceptionInfo
                   (ig/init-key init-key {}))))))
