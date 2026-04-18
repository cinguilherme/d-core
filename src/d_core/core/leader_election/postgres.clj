(ns d-core.core.leader-election.postgres
  (:require [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [d-core.core.leader-election.common :as common]
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

(defn- holder-row->response
  [status now-ms row]
  (when row
    [(name status)
     (:owner_id row)
     (str (:fencing row))
     (str (common/remaining-ttl-ms (:expires_at_ms row) now-ms))]))

(defn select-active-holder
  [datasource table-ident election-id now-ms]
  (query-one datasource
             [(str "SELECT owner_id, fencing, expires_at_ms "
                   "FROM " table-ident " "
                   "WHERE election_id = ? "
                   "AND token IS NOT NULL "
                   "AND expires_at_ms IS NOT NULL "
                   "AND expires_at_ms > ? "
                   "LIMIT 1")
              election-id
              now-ms]))

(defn try-acquire-row!
  [datasource table-ident election-id owner-id token now-ms lease-ms]
  (query-one datasource
             [(str "INSERT INTO " table-ident " AS le "
                   "(election_id, owner_id, token, fencing, acquired_at_ms, renewed_at_ms, expires_at_ms) "
                   "VALUES (?, ?, ?, 1, ?, ?, ?) "
                   "ON CONFLICT (election_id) DO UPDATE SET "
                   "owner_id = EXCLUDED.owner_id, "
                   "token = EXCLUDED.token, "
                   "fencing = le.fencing + 1, "
                   "acquired_at_ms = EXCLUDED.acquired_at_ms, "
                   "renewed_at_ms = EXCLUDED.renewed_at_ms, "
                   "expires_at_ms = EXCLUDED.expires_at_ms "
                   "WHERE le.token IS NULL "
                   "OR le.expires_at_ms IS NULL "
                   "OR le.expires_at_ms <= ? "
                   "RETURNING owner_id, fencing, token")
              election-id
              owner-id
              token
              now-ms
              now-ms
              (+ now-ms lease-ms)
              now-ms]))

(defn try-renew-row!
  [datasource table-ident election-id token now-ms lease-ms]
  (query-one datasource
             [(str "UPDATE " table-ident " "
                   "SET renewed_at_ms = ?, expires_at_ms = ? "
                   "WHERE election_id = ? "
                   "AND token = ? "
                   "AND token IS NOT NULL "
                   "AND expires_at_ms IS NOT NULL "
                   "AND expires_at_ms > ? "
                   "RETURNING owner_id, fencing, token")
              now-ms
              (+ now-ms lease-ms)
              election-id
              token
              now-ms]))

(defn try-resign-row!
  [datasource table-ident election-id token now-ms]
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
                   "AND expires_at_ms > ? "
                   "RETURNING fencing")
              election-id
              token
              now-ms]))

(defrecord PostgresLeaderElection [datasource owner-id table-ident default-lease-ms clock]
  p/LeaderElectionProtocol
  (acquire! [_ election-id opts]
    (let [election-id (common/normalize-election-id election-id)
          token (common/generate-token)
          now-ms (common/now-ms clock)
          lease-ms (common/lease-ms opts default-lease-ms)]
      (if-let [row (try-acquire-row! datasource table-ident election-id owner-id token now-ms lease-ms)]
        (common/acquire-result :postgres election-id
                               ["acquired"
                                (:owner_id row)
                                (str (:fencing row))
                                (:token row)
                                (str lease-ms)])
        (if-let [holder (select-active-holder datasource table-ident election-id now-ms)]
          (common/acquire-result :postgres election-id
                                 ["busy"
                                  (:owner_id holder)
                                  (str (:fencing holder))
                                  ""
                                  (str (common/remaining-ttl-ms (:expires_at_ms holder) now-ms))])
          (common/acquire-result :postgres election-id ["busy"])))))

  (renew! [_ election-id token opts]
    (let [election-id (common/normalize-election-id election-id)
          token (common/normalize-token token)
          now-ms (common/now-ms clock)
          lease-ms (common/lease-ms opts default-lease-ms)]
      (if-let [row (try-renew-row! datasource table-ident election-id token now-ms lease-ms)]
        (common/renew-result :postgres election-id
                             ["renewed"
                              (:owner_id row)
                              (str (:fencing row))
                              (:token row)
                              (str lease-ms)])
        (if-let [holder (select-active-holder datasource table-ident election-id now-ms)]
          (common/renew-result :postgres election-id
                               (holder-row->response :lost now-ms holder))
          (common/renew-result :postgres election-id ["lost"])))))

  (resign! [_ election-id token _opts]
    (let [election-id (common/normalize-election-id election-id)
          token (common/normalize-token token)
          now-ms (common/now-ms clock)]
      (if-let [row (try-resign-row! datasource table-ident election-id token now-ms)]
        (common/resign-result :postgres election-id
                              ["released"
                               owner-id
                               (str (:fencing row))])
        (if-let [holder (select-active-holder datasource table-ident election-id now-ms)]
          (common/resign-result :postgres election-id
                                (holder-row->response :not-owner now-ms holder))
          (common/resign-result :postgres election-id ["not-owner"])))))

  (status [_ election-id _opts]
    (let [election-id (common/normalize-election-id election-id)
          now-ms (common/now-ms clock)]
      (if-let [holder (select-active-holder datasource table-ident election-id now-ms)]
        (common/status-result :postgres election-id
                              (holder-row->response :held now-ms holder))
        (common/status-result :postgres election-id ["vacant"])))))

(defmethod ig/init-key :d-core.core.leader-election.postgres/postgres
  [_ {:keys [postgres-client owner-id default-lease-ms clock bootstrap-schema? table-name]
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
                              (common/normalize-clock clock))))
