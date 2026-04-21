(ns d-core.core.leader-election.logics.zookeeper-test
  (:require [clojure.test :refer [deftest is]]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.logics.zookeeper :as zk-logics]))

(def ^:private fixed-now-ms
  1700000000000)

(defn- basename
  [path]
  (last (clojure.string/split path #"/")))

(defn- local-record
  [ownership election-id]
  (get @ownership election-id))

(defn- assoc-local-record!
  [ownership election-id record]
  (swap! ownership assoc election-id record)
  record)

(defn- clear-local-record!
  [ownership election-id]
  (swap! ownership dissoc election-id)
  nil)

(defn- result-parts
  [status leader]
  [(name status)
   (:owner-id leader)
   (some-> (:fencing leader) str)])

(deftest acquire-stores-local-ownership-when-candidate-wins
  (let [ownership (atom {})
        ctx {:backend :zookeeper
             :zookeeper-client :client
             :owner-id "node-a"
             :base-path "/dcore/leader-election"
             :default-lease-ms 15000
             :clock (constantly fixed-now-ms)
             :ownership ownership
             :election-path (fn [_base election-id] (str "/dcore/leader-election/" election-id))
             :compatible-lease-ms (fn [_opts default-lease-ms] default-lease-ms)
             :connected-state! (fn [_client _op _election-id] true)
             :create-candidate! (fn [_client _election-path _payload]
                                  "/dcore/leader-election/orders/candidate-0000000001")
             :candidate-payload (fn [_owner-id _token _election-id _created-at-ms] (.getBytes "{}" "UTF-8"))
             :basename basename
             :leader-info (fn [_client _election-path]
                            {:name "candidate-0000000001"
                             :fencing 1})
             :assoc-local-record! assoc-local-record!
             :delete-node! (fn [_client _path] true)
             :clear-local-record! clear-local-record!
             :result-parts result-parts}]
    (with-redefs [common/generate-token (fn [] "token-1")]
      (is (= {:ok true
              :status :acquired
              :backend :zookeeper
              :election-id "orders"
              :owner-id "node-a"
              :fencing 1
              :token "token-1"}
             (zk-logics/acquire! ctx "orders" nil))))
    (is (= {"orders" {:token "token-1"
                      :path "/dcore/leader-election/orders/candidate-0000000001"
                      :fencing 1}}
           @ownership))))

(deftest renew-clears-local-ownership-when-foreign-leader-takes-over
  (let [ownership (atom {"orders" {:token "token-1"
                                   :path "/dcore/leader-election/orders/candidate-0000000001"
                                   :fencing 1}})
        ctx {:backend :zookeeper
             :zookeeper-client :client
             :owner-id "node-a"
             :base-path "/dcore/leader-election"
             :default-lease-ms 15000
             :ownership ownership
             :election-path (fn [_base election-id] (str "/dcore/leader-election/" election-id))
             :compatible-lease-ms (fn [_opts default-lease-ms] default-lease-ms)
             :safe-state? (fn [_client] true)
             :local-record local-record
             :basename basename
             :leader-info (fn [_client _election-path]
                            {:name "candidate-0000000002"
                             :owner-id "node-b"
                             :fencing 2})
             :clear-local-record! clear-local-record!
             :result-parts result-parts}]
    (is (= {:ok true
            :status :lost
            :backend :zookeeper
            :election-id "orders"
            :owner-id "node-b"
            :fencing 2}
           (zk-logics/renew! ctx "orders" "token-1" nil)))
    (is (empty? @ownership))))
