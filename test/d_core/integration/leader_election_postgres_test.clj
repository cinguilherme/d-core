(ns d-core.integration.leader-election-postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.postgres.client :as postgres-client]
            [d-core.core.leader-election.protocol :as p]
            [integrant.core :as ig]
            [next.jdbc :as jdbc])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_LEADER_ELECTION"))
      (some? (System/getenv "DCORE_INTEGRATION_LEADER_ELECTION_POSTGRES"))))

(defn- postgres-jdbc-url
  []
  (or (System/getenv "DCORE_POSTGRES_JDBC_URL")
      "jdbc:postgresql://localhost:5432/core-service"))

(defn- postgres-username
  []
  (or (System/getenv "DCORE_POSTGRES_USERNAME")
      "postgres"))

(defn- postgres-password
  []
  (or (System/getenv "DCORE_POSTGRES_PASSWORD")
      "postgres"))

(defn- cleanup-election!
  [datasource election-id]
  (jdbc/execute! datasource
                 ["DELETE FROM dcore_leader_elections WHERE election_id = ?"
                  election-id]))

(deftest postgres-leader-election-roundtrip
  (testing "postgres backend preserves leader-election semantics"
    (if-not (integration-enabled?)
      (is true "Skipping Postgres leader-election integration test; set INTEGRATION=1")
      (let [client (postgres-client/make-client {:jdbc-url (postgres-jdbc-url)
                                                 :username (postgres-username)
                                                 :password (postgres-password)})
            election-id (str "dcore:int:leader-election:postgres:" (UUID/randomUUID))
            datasource (:datasource client)
            leader-a (ig/init-key :d-core.core.leader-election.postgres/postgres
                                  {:postgres-client client
                                   :owner-id "node-a"
                                   :bootstrap-schema? true})
            leader-b (ig/init-key :d-core.core.leader-election.postgres/postgres
                                  {:postgres-client client
                                   :owner-id "node-b"})]
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
                    :backend :postgres
                    :election-id election-id}
                   (p/status leader-a election-id nil))))

          (let [first-acquire (p/acquire! leader-a election-id {:lease-ms 100})
                first-fencing (:fencing first-acquire)]
            (is (= :acquired (:status first-acquire)))
            (Thread/sleep 150)
            (is (= :vacant (:status (p/status leader-a election-id nil))))
            (let [second-acquire (p/acquire! leader-b election-id {:lease-ms 100})]
              (is (= :acquired (:status second-acquire)))
              (is (> (:fencing second-acquire) first-fencing))
              (is (= "node-b" (:owner-id second-acquire)))
              (is (= :released
                     (:status (p/resign! leader-b election-id (:token second-acquire) nil))))))
          (finally
            (cleanup-election! datasource election-id)
            (ig/halt-key! :d-core.core.leader-election.postgres/postgres leader-a)
            (ig/halt-key! :d-core.core.leader-election.postgres/postgres leader-b)
            (postgres-client/close! client)))))))
