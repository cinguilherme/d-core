(ns d-core.core.leader-election.postgres
  (:require [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.logics.postgres :as pg-logics]
            [d-core.core.leader-election.observability :as obs]
            [d-core.core.leader-election.protocol :as p]))

(def default-table-name
  "dcore_leader_elections")

(defn- ident
  [value {:keys [kind]}]
  (let [s (cond
            (keyword? value) (name value)
            (string? value) value
            :else (throw (ex-info "Identifier must be keyword or string"
                                  {:type ::invalid-identifier
                                   :kind kind
                                   :value value})))]
    (when-not (re-matches #"[A-Za-z_][A-Za-z0-9_]*" s)
      (throw (ex-info "Unsafe identifier"
                      {:type ::unsafe-identifier
                       :kind kind
                       :value value})))
    (str "\"" s "\"")))

(defn normalize-table-name
  [table-name]
  (ident (or table-name default-table-name) {:kind :table}))

(defn- query-one
  [datasource sqlvec]
  (first (jdbc/execute! datasource sqlvec {:builder-fn rs/as-unqualified-lower-maps})))

(defn ddl-statements
  [table-ident]
  [(str "CREATE TABLE IF NOT EXISTS " table-ident " ("
        "election_id TEXT PRIMARY KEY, "
        "owner_id TEXT NULL, "
        "token TEXT NULL, "
        "fencing BIGINT NOT NULL DEFAULT 0, "
        "acquired_at_ms BIGINT NULL, "
        "renewed_at_ms BIGINT NULL, "
        "expires_at_ms BIGINT NULL)")])

(defn bootstrap-schema!
  [datasource table-ident]
  (doseq [ddl (ddl-statements table-ident)]
    (jdbc/execute! datasource [ddl]))
  {:ok true})

(def ^:private statement-now-ms-sql
  "(floor(extract(epoch from statement_timestamp()) * 1000))::bigint")

(defn- remaining-ttl-sql
  []
  (str "GREATEST(0, expires_at_ms - " statement-now-ms-sql ")::bigint AS remaining_ttl_ms"))

(defn select-active-holder
  [datasource table-ident election-id]
  (query-one datasource
             [(str "SELECT owner_id, fencing, " (remaining-ttl-sql) " "
                   "FROM " table-ident " "
                   "WHERE election_id = ? "
                   "AND token IS NOT NULL "
                   "AND expires_at_ms IS NOT NULL "
                   "AND expires_at_ms > " statement-now-ms-sql " "
                   "LIMIT 1")
              election-id]))

(defn try-acquire-row!
  [datasource table-ident election-id owner-id token lease-ms]
  (query-one datasource
             [(str "INSERT INTO " table-ident " AS le "
                   "(election_id, owner_id, token, fencing, acquired_at_ms, renewed_at_ms, expires_at_ms) "
                   "VALUES (?, ?, ?, 1, "
                   statement-now-ms-sql ", "
                   statement-now-ms-sql ", "
                   statement-now-ms-sql " + ?) "
                   "ON CONFLICT (election_id) DO UPDATE SET "
                   "owner_id = EXCLUDED.owner_id, "
                   "token = EXCLUDED.token, "
                   "fencing = le.fencing + 1, "
                   "acquired_at_ms = EXCLUDED.acquired_at_ms, "
                   "renewed_at_ms = EXCLUDED.renewed_at_ms, "
                   "expires_at_ms = EXCLUDED.expires_at_ms "
                   "WHERE le.token IS NULL "
                   "OR le.expires_at_ms IS NULL "
                   "OR le.expires_at_ms <= " statement-now-ms-sql " "
                   "RETURNING owner_id, fencing, token, " (remaining-ttl-sql))
              election-id
              owner-id
              token
              lease-ms]))

(defn try-renew-row!
  [datasource table-ident election-id token lease-ms]
  (query-one datasource
             [(str "UPDATE " table-ident " "
                   "SET renewed_at_ms = " statement-now-ms-sql ", "
                   "expires_at_ms = " statement-now-ms-sql " + ? "
                   "WHERE election_id = ? "
                   "AND token = ? "
                   "AND token IS NOT NULL "
                   "AND expires_at_ms IS NOT NULL "
                   "AND expires_at_ms > " statement-now-ms-sql " "
                   "RETURNING owner_id, fencing, token, " (remaining-ttl-sql))
              lease-ms
              election-id
              token]))

(defn try-resign-row!
  [datasource table-ident election-id token]
  (query-one datasource
             [(str "UPDATE " table-ident " "
                   "SET owner_id = NULL, "
                   "token = NULL, "
                   "acquired_at_ms = NULL, "
                   "renewed_at_ms = NULL, "
                   "expires_at_ms = NULL "
                   "WHERE election_id = ? "
                   "AND token = ? "
                   "AND token IS NOT NULL "
                   "AND expires_at_ms IS NOT NULL "
                   "AND expires_at_ms > " statement-now-ms-sql " "
                   "RETURNING fencing")
              election-id
              token]))

(defrecord PostgresLeaderElection [datasource owner-id table-ident default-lease-ms clock observability]
  p/LeaderElectionProtocol
  (acquire! [_ election-id opts]
    (obs/observe-operation observability :postgres :acquire election-id
                           (fn []
                             (pg-logics/acquire! {:backend :postgres
                                                  :datasource datasource
                                                  :table-ident table-ident
                                                  :owner-id owner-id
                                                  :default-lease-ms default-lease-ms
                                                  :try-acquire-row! try-acquire-row!
                                                  :select-active-holder select-active-holder}
                                                 election-id
                                                 opts))))

  (renew! [_ election-id token opts]
    (obs/observe-operation observability :postgres :renew election-id
                           (fn []
                             (pg-logics/renew! {:backend :postgres
                                                :datasource datasource
                                                :table-ident table-ident
                                                :default-lease-ms default-lease-ms
                                                :try-renew-row! try-renew-row!
                                                :select-active-holder select-active-holder}
                                               election-id
                                               token
                                               opts))))

  (resign! [_ election-id token _opts]
    (obs/observe-operation observability :postgres :resign election-id
                           (fn []
                             (pg-logics/resign! {:backend :postgres
                                                 :datasource datasource
                                                 :table-ident table-ident
                                                 :owner-id owner-id
                                                 :try-resign-row! try-resign-row!
                                                 :select-active-holder select-active-holder}
                                                election-id
                                                token))))

  (status [_ election-id _opts]
    (obs/observe-operation observability :postgres :status election-id
                           (fn []
                             (pg-logics/status {:backend :postgres
                                                :datasource datasource
                                                :table-ident table-ident
                                                :select-active-holder select-active-holder}
                                               election-id)))))

(defmethod ig/init-key :d-core.core.leader-election.postgres/postgres
  [_ {:keys [postgres-client owner-id default-lease-ms clock bootstrap-schema? table-name logger metrics]
      :or {default-lease-ms common/default-lease-ms
           bootstrap-schema? false
           table-name default-table-name}}]
  (when-not postgres-client
    (throw (ex-info "Postgres leader election requires :postgres-client"
                    {:type ::missing-postgres-client})))
  (let [datasource (:datasource postgres-client)
        table-ident (normalize-table-name table-name)]
    (when bootstrap-schema?
      (bootstrap-schema! datasource table-ident))
    (->PostgresLeaderElection datasource
                              (common/normalize-owner-id owner-id)
                              table-ident
                              (common/require-positive-long default-lease-ms :default-lease-ms)
                              (common/normalize-clock clock)
                              (obs/make-context logger metrics))))
