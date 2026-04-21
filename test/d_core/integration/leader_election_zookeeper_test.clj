(ns d-core.integration.leader-election-zookeeper-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.zookeeper.client :as zk-client]
            [d-core.core.leader-election.protocol :as p]
            [d-core.core.leader-election.zookeeper]
            [integrant.core :as ig])
  (:import [java.util UUID]
           [org.apache.zookeeper KeeperException$NoNodeException]))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_LEADER_ELECTION"))
      (some? (System/getenv "DCORE_INTEGRATION_LEADER_ELECTION_ZOOKEEPER"))))

(defn- zookeeper-connect-string
  []
  (or (System/getenv "DCORE_ZOOKEEPER_CONNECT_STRING")
      "localhost:2181"))

(defn- wait-for
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if-let [value (pred)]
        value
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 50) (recur))
          false)))))

(defn- cleanup-base-path!
  [client base-path]
  (try
    (.. (:curator client) delete deletingChildrenIfNeeded (forPath base-path))
    (catch KeeperException$NoNodeException _
      nil)))

(defn- make-client
  []
  (zk-client/make-client {:connect-string (zookeeper-connect-string)
                          :session-timeout-ms 5000
                          :connection-timeout-ms 3000
                          :block-until-connected-ms 5000}))

(defn- make-election
  [client owner-id base-path]
  (ig/init-key :d-core.core.leader-election.zookeeper/zookeeper
               {:zookeeper-client client
                :owner-id owner-id
                :base-path base-path
                :default-lease-ms 5000}))

(defn- run-roundtrip!
  []
  (let [client-a (make-client)
        client-b (make-client)
        base-path (str "/dcore/int/leader-election/zookeeper/" (UUID/randomUUID))
        election-id (str "orders-" (UUID/randomUUID))
        leader-a (make-election client-a "node-a" base-path)
        leader-b (make-election client-b "node-b" base-path)]
    (try
      (let [acquired (p/acquire! leader-a election-id nil)
            busy (p/acquire! leader-b election-id nil)]
        (is (= :acquired (:status acquired)))
        (is (= "node-a" (:owner-id acquired)))
        (is (string? (:token acquired)))
        (is (>= (:fencing acquired) 0))
        (is (false? (contains? acquired :remaining-ttl-ms)))

        (is (= :busy (:status busy)))
        (is (= "node-a" (:owner-id busy)))
        (is (= (:fencing acquired) (:fencing busy)))

        (is (= {:ok true
                :status :held
                :backend :zookeeper
                :election-id election-id
                :owner-id "node-a"
                :fencing (:fencing acquired)}
               (p/status leader-a election-id nil)))

        (let [renewed (p/renew! leader-a election-id (:token acquired) nil)]
          (is (= :renewed (:status renewed)))
          (is (= (:token acquired) (:token renewed)))
          (is (= (:fencing acquired) (:fencing renewed))))

        (let [lost (p/renew! leader-b election-id "wrong-token" nil)]
          (is (= :lost (:status lost)))
          (is (= "node-a" (:owner-id lost))))

        (let [not-owner (p/resign! leader-b election-id "wrong-token" nil)]
          (is (= :not-owner (:status not-owner)))
          (is (= "node-a" (:owner-id not-owner))))

        (let [released (p/resign! leader-a election-id (:token acquired) nil)]
          (is (= :released (:status released)))
          (is (= "node-a" (:owner-id released))))

        (is (= {:ok true
                :status :vacant
                :backend :zookeeper
                :election-id election-id}
               (p/status leader-a election-id nil))))

      (let [first-acquire (p/acquire! leader-a election-id nil)]
        (is (= :acquired (:status first-acquire)))
        (is (= :released
               (:status (p/resign! leader-a election-id (:token first-acquire) nil))))
        (let [second-acquire (p/acquire! leader-b election-id nil)]
          (is (= :acquired (:status second-acquire)))
          (is (> (:fencing second-acquire) (:fencing first-acquire)))
          (is (= :released
                 (:status (p/resign! leader-b election-id (:token second-acquire) nil))))))
      (finally
        (cleanup-base-path! client-b base-path)
        (zk-client/close! client-a)
        (zk-client/close! client-b)))))

(defn- run-client-close!
  []
  (let [client-a (make-client)
        client-b (make-client)
        base-path (str "/dcore/int/leader-election/zookeeper/" (UUID/randomUUID))
        election-id (str "orders-" (UUID/randomUUID))
        leader-a (make-election client-a "node-a" base-path)
        leader-b (make-election client-b "node-b" base-path)]
    (try
      (let [acquired-a (p/acquire! leader-a election-id nil)]
        (is (= :acquired (:status acquired-a)))
        (zk-client/close! client-a)
        (let [acquired-b (wait-for #(let [result (p/acquire! leader-b election-id nil)]
                                      (when (= :acquired (:status result))
                                        result))
                                   5000)]
          (is acquired-b)
          (is (= :acquired (:status acquired-b)))
          (is (= "node-b" (:owner-id acquired-b)))
          (is (> (:fencing acquired-b) (:fencing acquired-a)))
          (is (= {:ok true
                  :status :lost
                  :backend :zookeeper
                  :election-id election-id}
                 (p/renew! leader-a election-id (:token acquired-a) nil)))
          (is (= :released
                 (:status (p/resign! leader-b election-id (:token acquired-b) nil))))))
      (finally
        (cleanup-base-path! client-b base-path)
        (zk-client/close! client-b)))))

(deftest zookeeper-leader-election-roundtrip
  (testing "zookeeper backend preserves session-backed leader-election semantics"
    (if (integration-enabled?)
      (run-roundtrip!)
      (is true "Skipping ZooKeeper leader-election integration test; set INTEGRATION=1"))))

(deftest zookeeper-leadership-is-lost-when-owning-client-closes
  (testing "closing the owning session allows a new contender to acquire leadership"
    (if (integration-enabled?)
      (run-client-close!)
      (is true "Skipping ZooKeeper leadership-loss integration test; set INTEGRATION=1"))))
