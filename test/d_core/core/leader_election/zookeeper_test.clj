(ns d-core.core.leader-election.zookeeper-test
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.protocol :as p]
            [d-core.core.leader-election.zookeeper :as zk]
            [integrant.core :as ig]))

(def ^:private fixed-now-ms
  1700000000000)

(defn- payload-bytes
  [{:keys [owner-id token election-id created-at-ms]}]
  (.getBytes (json/generate-string {:owner-id owner-id
                                    :token token
                                    :election-id election-id
                                    :created-at-ms (or created-at-ms fixed-now-ms)})
             "UTF-8"))

(defn- make-client
  ([]
   (make-client :connected))
  ([state]
   {:curator :curator
    :session-timeout-ms 15000
    :connection-state (atom state)}))

(defn- make-component
  ([] (make-component nil))
  ([client]
   (zk/->ZooKeeperLeaderElection (or client (make-client))
                                 "node-a"
                                 zk/default-base-path
                                 15000
                                 (constantly fixed-now-ms)
                                 nil
                                 (atom {}))))

(deftest election-path-segment-is-deterministic-and-safe
  (let [segment-a (zk/election-path-segment "Orders/Sync Primary")
        segment-b (zk/election-path-segment "Orders/Sync Primary")]
    (is (= segment-a segment-b))
    (is (re-matches #"[a-z0-9-]+" segment-a))
    (is (<= (count segment-a) 96))
    (is (= (str "/root/" segment-a)
           (zk/election-path "/root" "Orders/Sync Primary")))))

(deftest acquire-returns-acquired-when-own-candidate-is-smallest
  (let [component (make-component)]
    (with-redefs [d-core.core.leader-election.common/generate-token (fn [] "token-1")
                  zk/create-candidate! (fn [_client _election-path _payload]
                                         "/dcore/leader-election/orders/candidate-0000000007")
                  zk/list-children (fn [_client _election-path]
                                     ["candidate-0000000007"])
                  zk/read-node-data (fn [_client path]
                                      (when (str/ends-with? path "/candidate-0000000007")
                                        (payload-bytes {:owner-id "node-a"
                                                        :token "token-1"
                                                        :election-id "orders"})))]
      (let [result (p/acquire! component :orders {:lease-ms 15000})]
        (is (= {:ok true
                :status :acquired
                :backend :zookeeper
                :election-id "orders"
                :owner-id "node-a"
                :fencing 7
                :token "token-1"}
               result))
        (is (false? (contains? result :remaining-ttl-ms)))
        (is (= {:token "token-1"
                :path "/dcore/leader-election/orders/candidate-0000000007"
                :fencing 7}
               (get @(:ownership component) "orders")))))))

(deftest acquire-busy-deletes-own-candidate
  (let [component (make-component)
        deleted (atom [])]
    (with-redefs [d-core.core.leader-election.common/generate-token (fn [] "token-2")
                  zk/create-candidate! (fn [_client _election-path _payload]
                                         "/dcore/leader-election/orders/candidate-0000000008")
                  zk/list-children (fn [_client _election-path]
                                     ["candidate-0000000007" "candidate-0000000008"])
                  zk/read-node-data (fn [_client path]
                                      (when (str/ends-with? path "/candidate-0000000007")
                                        (payload-bytes {:owner-id "node-b"
                                                        :token "token-b"
                                                        :election-id "orders"})))
                  zk/delete-node! (fn [_client path]
                                    (swap! deleted conj path)
                                    true)]
      (let [result (p/acquire! component "orders" nil)]
        (is (= {:ok true
                :status :busy
                :backend :zookeeper
                :election-id "orders"
                :owner-id "node-b"
                :fencing 7}
               result))
        (is (= ["/dcore/leader-election/orders/candidate-0000000008"] @deleted))
        (is (empty? @(:ownership component)))))))

(deftest renew-success-and-loss-contracts
  (testing "renew succeeds while local candidate remains leader"
    (let [component (make-component)]
      (reset! (:ownership component)
              {"orders" {:token "token-1"
                         :path "/dcore/leader-election/orders/candidate-0000000007"
                         :fencing 7}})
      (with-redefs [zk/list-children (fn [_client _election-path]
                                       ["candidate-0000000007"])
                    zk/read-node-data (fn [_client path]
                                        (when (str/ends-with? path "/candidate-0000000007")
                                          (payload-bytes {:owner-id "node-a"
                                                          :token "token-1"
                                                          :election-id "orders"})))]
        (let [result (p/renew! component :orders "token-1" {:lease-ms 15000})]
          (is (= {:ok true
                  :status :renewed
                  :backend :zookeeper
                  :election-id "orders"
                  :owner-id "node-a"
                  :fencing 7
                  :token "token-1"}
                 result))
          (is (false? (contains? result :remaining-ttl-ms)))))))

  (testing "renew returns lost and clears local ownership when leader changed"
    (let [component (make-component)]
      (reset! (:ownership component)
              {"orders" {:token "token-1"
                         :path "/dcore/leader-election/orders/candidate-0000000007"
                         :fencing 7}})
      (with-redefs [zk/list-children (fn [_client _election-path]
                                       ["candidate-0000000008"])
                    zk/read-node-data (fn [_client path]
                                        (when (str/ends-with? path "/candidate-0000000008")
                                          (payload-bytes {:owner-id "node-b"
                                                          :token "token-b"
                                                          :election-id "orders"})))]
        (is (= {:ok true
                :status :lost
                :backend :zookeeper
                :election-id "orders"
                :owner-id "node-b"
                :fencing 8}
               (p/renew! component "orders" "token-1" nil)))
        (is (empty? @(:ownership component)))))))

(deftest resign-contracts
  (testing "resign deletes owned candidate"
    (let [component (make-component)
          deleted (atom [])]
      (reset! (:ownership component)
              {"orders" {:token "token-1"
                         :path "/dcore/leader-election/orders/candidate-0000000007"
                         :fencing 7}})
      (with-redefs [zk/list-children (fn [_client _election-path]
                                       ["candidate-0000000007"])
                    zk/read-node-data (fn [_client path]
                                        (when (str/ends-with? path "/candidate-0000000007")
                                          (payload-bytes {:owner-id "node-a"
                                                          :token "token-1"
                                                          :election-id "orders"})))
                    zk/delete-node! (fn [_client path]
                                      (swap! deleted conj path)
                                      true)]
        (is (= {:ok true
                :status :released
                :backend :zookeeper
                :election-id "orders"
                :owner-id "node-a"
                :fencing 7}
               (p/resign! component "orders" "token-1" nil)))
        (is (= ["/dcore/leader-election/orders/candidate-0000000007"] @deleted))
        (is (empty? @(:ownership component))))))

  (testing "wrong token returns not-owner"
    (let [component (make-component)]
      (reset! (:ownership component)
              {"orders" {:token "token-1"
                         :path "/dcore/leader-election/orders/candidate-0000000007"
                         :fencing 7}})
      (with-redefs [zk/list-children (fn [_client _election-path]
                                       ["candidate-0000000008"])
                    zk/read-node-data (fn [_client path]
                                        (when (str/ends-with? path "/candidate-0000000008")
                                          (payload-bytes {:owner-id "node-b"
                                                          :token "token-b"
                                                          :election-id "orders"})))]
        (is (= {:ok true
                :status :not-owner
                :backend :zookeeper
                :election-id "orders"
                :owner-id "node-b"
                :fencing 8}
               (p/resign! component "orders" "wrong-token" nil)))))))

(deftest status-contracts
  (testing "status is held when a leader exists"
    (let [component (make-component)]
      (with-redefs [zk/list-children (fn [_client _election-path]
                                       ["candidate-0000000007"])
                    zk/read-node-data (fn [_client path]
                                        (when (str/ends-with? path "/candidate-0000000007")
                                          (payload-bytes {:owner-id "node-b"
                                                          :token "token-b"
                                                          :election-id "orders"})))]
        (let [result (p/status component "orders" nil)]
          (is (= {:ok true
                  :status :held
                  :backend :zookeeper
                  :election-id "orders"
                  :owner-id "node-b"
                  :fencing 7}
                 result))
          (is (false? (contains? result :remaining-ttl-ms)))
          (is (false? (contains? result :token)))))))

  (testing "status is vacant when no candidates exist"
    (let [component (make-component)]
      (with-redefs [zk/list-children (fn [_client _election-path] nil)]
        (is (= {:ok true
                :status :vacant
                :backend :zookeeper
                :election-id "orders"}
               (p/status component "orders" nil))))))

  (testing "malformed foreign leader data does not leak token and can omit owner id"
    (let [component (make-component)]
      (with-redefs [zk/list-children (fn [_client _election-path]
                                       ["candidate-0000000007"])
                    zk/read-node-data (fn [_client _path]
                                        (.getBytes "not-json" "UTF-8"))]
        (let [result (p/status component "orders" nil)]
          (is (= {:ok true
                  :status :held
                  :backend :zookeeper
                  :election-id "orders"
                  :fencing 7}
                 result))
          (is (false? (contains? result :token))))))))

(deftest session-backed-lease-validation
  (let [component (make-component)]
    (testing "different per-call lease-ms is rejected"
      (let [error (try
                    (p/acquire! component "orders" {:lease-ms 12000})
                    nil
                    (catch clojure.lang.ExceptionInfo ex
                      ex))]
        (is (= ::zk/unsupported-lease-ms (:type (ex-data error))))))

    (testing "exact-match per-call lease-ms is accepted"
      (with-redefs [d-core.core.leader-election.common/generate-token (fn [] "token-1")
                    zk/create-candidate! (fn [_client _election-path _payload]
                                           "/dcore/leader-election/orders/candidate-0000000007")
                    zk/list-children (fn [_client _election-path]
                                       ["candidate-0000000007"])
                    zk/read-node-data (fn [_client _path]
                                        (payload-bytes {:owner-id "node-a"
                                                        :token "token-1"
                                                        :election-id "orders"}))]
        (is (= :acquired
               (:status (p/acquire! component "orders" {:lease-ms 15000}))))))))

(deftest conservative-connection-state-behavior
  (testing "renew loses leadership on suspended or lost connections"
    (doseq [state [:suspended :lost]]
      (let [component (make-component (make-client state))]
        (reset! (:ownership component)
                {"orders" {:token "token-1"
                           :path "/dcore/leader-election/orders/candidate-0000000007"
                           :fencing 7}})
        (is (= {:ok true
                :status :lost
                :backend :zookeeper
                :election-id "orders"}
               (p/renew! component "orders" "token-1" nil)))
        (is (empty? @(:ownership component))))))

  (testing "acquire and status require a connected client"
    (doseq [op [(fn [component] (p/acquire! component "orders" nil))
                (fn [component] (p/status component "orders" nil))]]
      (let [component (make-component (make-client :suspended))
            error (try
                    (op component)
                    nil
                    (catch clojure.lang.ExceptionInfo ex
                      ex))]
        (is (= ::zk/not-connected (:type (ex-data error))))))))

(deftest init-key-validates-session-timeout-compatibility
  (testing "default lease inherits the client session timeout"
    (let [ensured (atom [])]
      (with-redefs [zk/ensure-path! (fn [_client path]
                                      (swap! ensured conj path)
                                      path)]
        (let [component (ig/init-key :d-core.core.leader-election.zookeeper/zookeeper
                                     {:zookeeper-client (make-client)
                                      :owner-id "node-a"})]
          (is (= 15000 (:default-lease-ms component)))
          (is (= [zk/default-base-path] @ensured))))))

  (testing "explicit default lease must match the session timeout"
    (with-redefs [zk/ensure-path! (fn [_client path] path)]
      (let [error (try
                    (ig/init-key :d-core.core.leader-election.zookeeper/zookeeper
                                 {:zookeeper-client (make-client)
                                  :default-lease-ms 12000})
                    nil
                    (catch clojure.lang.ExceptionInfo ex
                      ex))]
        (is (= ::zk/default-lease-ms-mismatch (:type (ex-data error))))))))
