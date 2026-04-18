(ns d-core.integration.leader-election-backends-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.protocol :as p]
            [d-core.core.leader-election.redis-common :as redis-common]
            [d-core.core.leader-election.redis :as redis]
            [d-core.core.leader-election.valkey :as valkey]
            [d-core.core.clients.redis.client :as redis-client]
            [d-core.core.clients.valkey.client :as valkey-client]
            [taoensso.carmine :as car])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_LEADER_ELECTION"))))

(def ^:private backend-cases
  [{:name :redis
    :env-var "DCORE_INTEGRATION_LEADER_ELECTION_REDIS"
    :uri-env "DCORE_REDIS_URI"
    :default-uri "redis://localhost:6379"
    :make-client redis-client/make-client
    :make-component (fn [client owner-id prefix]
                      (redis/->RedisLeaderElection client owner-id prefix 200 #(System/currentTimeMillis)))}
   {:name :valkey
    :env-var "DCORE_INTEGRATION_LEADER_ELECTION_VALKEY"
    :uri-env "DCORE_VALKEY_URI"
    :default-uri "redis://localhost:6380"
    :make-client valkey-client/make-client
    :make-component (fn [client owner-id prefix]
                      (valkey/->ValkeyLeaderElection client owner-id prefix 200 #(System/currentTimeMillis)))}])

(defn- backend-enabled?
  [env-var]
  (or (integration-enabled?)
      (some? (System/getenv env-var))))

(defn- backend-uri
  [{:keys [uri-env default-uri]}]
  (or (System/getenv uri-env)
      default-uri))

(defn- del-key!
  [client key]
  (car/wcar (:conn client)
            (car/del key)))

(defn- cleanup-election!
  [client prefix election-id]
  (doseq [key [(redis-common/lease-key prefix election-id)
               (redis-common/fencing-key prefix election-id)]]
    (del-key! client key)))

(defn- wait-for
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 20) (recur))
          false)))))

(deftest leader-election-backends-roundtrip
  (doseq [{backend-name :name :keys [env-var make-client make-component] :as backend} backend-cases]
    (testing (str backend-name " leader election roundtrip")
      (if-not (backend-enabled? env-var)
        (is true (str "Skipping " (clojure.core/name backend-name) " integration test; set INTEGRATION=1"))
        (let [client (make-client {:uri (backend-uri backend)})
              prefix (str "dcore:int:leader-election:" (UUID/randomUUID) ":")
              election-id "orders"
              leader-a (make-component client "node-a" prefix)
              leader-b (make-component client "node-b" prefix)]
          (try
            (let [acquired (p/acquire! leader-a election-id {:lease-ms 200})]
              (is (= :acquired (:status acquired)))
              (is (= "node-a" (:owner-id acquired)))
              (is (string? (:token acquired)))
              (is (pos? (:fencing acquired)))

              (let [busy (p/acquire! leader-b election-id {:lease-ms 200})]
                (is (= :busy (:status busy)))
                (is (= "node-a" (:owner-id busy)))
                (is (= (:fencing acquired) (:fencing busy)))
                (is (false? (contains? busy :token))))

              (let [held (p/status leader-a election-id nil)]
                (is (= :held (:status held)))
                (is (= "node-a" (:owner-id held)))
                (is (false? (contains? held :token))))

              (let [renewed (p/renew! leader-a election-id (:token acquired) {:lease-ms 200})]
                (is (= :renewed (:status renewed)))
                (is (= (:token acquired) (:token renewed)))
                (is (= (:fencing acquired) (:fencing renewed))))

              (let [lost (p/renew! leader-b election-id "wrong-token" {:lease-ms 200})]
                (is (= :lost (:status lost)))
                (is (= "node-a" (:owner-id lost)))
                (is (false? (contains? lost :token))))

              (let [not-owner (p/resign! leader-b election-id "wrong-token" nil)]
                (is (= :not-owner (:status not-owner)))
                (is (= "node-a" (:owner-id not-owner))))

              (let [released (p/resign! leader-a election-id (:token acquired) nil)]
                (is (= :released (:status released)))
                (is (= "node-a" (:owner-id released))))

              (is (= {:ok true
                      :status :vacant
                      :backend backend-name
                      :election-id election-id}
                     (p/status leader-a election-id nil))))

            (let [first-acquire (p/acquire! leader-a election-id {:lease-ms 250})
                  first-fencing (:fencing first-acquire)]
              (is (= :acquired (:status first-acquire)))
              (is (wait-for #(= :vacant (:status (p/status leader-a election-id nil))) 2000))
              (let [second-acquire (p/acquire! leader-b election-id {:lease-ms 250})]
                (is (= :acquired (:status second-acquire)))
                (is (> (:fencing second-acquire) first-fencing))
                (is (= "node-b" (:owner-id second-acquire)))
                (p/resign! leader-b election-id (:token second-acquire) nil)))
            (finally
              (cleanup-election! client prefix election-id))))))))
