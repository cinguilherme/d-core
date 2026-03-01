(ns d-core.core.api-keys.postgres
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [d-core.core.api-keys.protocol :as p])
  (:import (java.security MessageDigest SecureRandom)
           (java.sql Timestamp)
           (java.time Instant)
           (java.util Base64 UUID)
           (java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit)
           (org.postgresql.util PGobject)))

(def ^:private key-prefix
  "dck_")

(def ^:private token-pattern
  #"^dck_([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]+)$")

(def ^:private secure-random
  (SecureRandom.))

(def ^:private ddl-statements
  ["CREATE TABLE IF NOT EXISTS dcore_api_keys (
      api_key_id UUID PRIMARY KEY,
      name TEXT NULL,
      tenant_id TEXT NOT NULL,
      scopes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      key_prefix TEXT NOT NULL UNIQUE,
      key_hash TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      expires_at TIMESTAMPTZ NULL,
      last_used_at TIMESTAMPTZ NULL,
      limits_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      revoked_at TIMESTAMPTZ NULL
    )"
   "CREATE INDEX IF NOT EXISTS idx_dcore_api_keys_tenant_status
      ON dcore_api_keys (tenant_id, status)"
   "CREATE INDEX IF NOT EXISTS idx_dcore_api_keys_expires_at
      ON dcore_api_keys (expires_at)"])

(defn- now-instant []
  (Instant/now))

(defn- tenant-id->str
  [tenant-id]
  (when tenant-id
    (if (keyword? tenant-id) (name tenant-id) (str tenant-id))))

(defn- normalize-scope-set
  [scopes]
  (cond
    (nil? scopes) #{}
    (set? scopes) (set (map str scopes))
    (string? scopes) (->> (str/split scopes #"\s+") (remove str/blank?) set)
    (coll? scopes) (->> scopes (map str) (remove str/blank?) set)
    :else #{(str scopes)}))

(defn- ->timestamp
  [value]
  (cond
    (nil? value) nil
    (instance? Timestamp value) value
    (instance? Instant value) (Timestamp/from ^Instant value)
    (instance? java.util.Date value) (Timestamp/from (.toInstant ^java.util.Date value))
    :else (throw (ex-info "Unsupported timestamp value"
                          {:type ::invalid-timestamp
                           :value value}))))

(defn- ->epoch-ms
  [value]
  (cond
    (nil? value) nil
    (instance? Timestamp value) (.toEpochMilli (.toInstant ^Timestamp value))
    (instance? Instant value) (.toEpochMilli ^Instant value)
    (instance? java.util.Date value) (.getTime ^java.util.Date value)
    :else nil))

(defn- row-get
  [row k]
  (or (get row k)
      (get row (keyword (str/replace (name k) "-" "_")))
      (get row (keyword (str/replace (name k) "_" "-")))))

(defn- maybe-json-string
  [value]
  (cond
    (nil? value) nil
    (string? value) value
    (instance? PGobject value) (.getValue ^PGobject value)
    :else (str value)))

(defn- decode-json
  [value default-value]
  (if-let [json-str (maybe-json-string value)]
    (try
      (json/parse-string json-str true)
      (catch Exception _
        default-value))
    default-value))

(defn- encode-json
  [value default-value]
  (json/generate-string (or value default-value)))

(defn- bytes->hex
  [^bytes bs]
  (let [sb (StringBuilder.)]
    (doseq [b bs]
      (.append sb (format "%02x" (bit-and b 0xff))))
    (str sb)))

(defn- sha256-hex
  [s]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (.update digest (.getBytes (str s) "UTF-8"))
    (bytes->hex (.digest digest))))

(defn- constant-time-equals?
  [a b]
  (MessageDigest/isEqual (.getBytes (str a) "UTF-8")
                         (.getBytes (str b) "UTF-8")))

(defn- random-token-part
  [n-bytes]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder))
                   (let [arr (byte-array n-bytes)]
                     (.nextBytes secure-random arr)
                     arr)))

(defn- generate-prefix
  []
  (subs (random-token-part 12) 0 16))

(defn- generate-secret
  []
  (random-token-part 32))

(defn- compose-token
  [prefix secret]
  (str key-prefix prefix "." secret))

(defn- parse-token
  [token]
  (when (string? token)
    (when-let [[_ prefix secret] (re-matches token-pattern token)]
      {:prefix prefix
       :secret secret})))

(defn- secret->hash
  [pepper secret]
  (sha256-hex (str (or pepper "") ":" secret)))

(defn- mask-prefix
  [prefix]
  (if (and prefix (>= (count prefix) 6))
    (str (subs prefix 0 6) "...")
    prefix))

(defn- sanitize-row
  [row]
  (when row
    (let [api-key-id (row-get row :api_key_id)
          status (row-get row :status)
          scopes (decode-json (row-get row :scopes_json) [])
          limits (decode-json (row-get row :limits_json) {})
          metadata (decode-json (row-get row :metadata_json) {})]
      {:api-key-id (some-> api-key-id str)
       :name (row-get row :name)
       :tenant-id (row-get row :tenant_id)
       :status (keyword (or status "active"))
       :scopes (normalize-scope-set scopes)
       :key-prefix (row-get row :key_prefix)
       :key-prefix-masked (mask-prefix (row-get row :key_prefix))
       :expires-at (row-get row :expires_at)
       :expires-at-epoch-ms (->epoch-ms (row-get row :expires_at))
       :last-used-at (row-get row :last_used_at)
       :last-used-at-epoch-ms (->epoch-ms (row-get row :last_used_at))
       :revoked-at (row-get row :revoked_at)
       :revoked-at-epoch-ms (->epoch-ms (row-get row :revoked_at))
       :created-at (row-get row :created_at)
       :created-at-epoch-ms (->epoch-ms (row-get row :created_at))
       :updated-at (row-get row :updated_at)
       :updated-at-epoch-ms (->epoch-ms (row-get row :updated_at))
       :limits (if (map? limits) limits {})
       :metadata (if (map? metadata) metadata {})})))

(defn- auth-valid?
  [row provided-hash]
  (let [status (str/lower-case (or (row-get row :status) ""))
        revoked-at (row-get row :revoked_at)
        expires-at (row-get row :expires_at)
        now (now-instant)]
    (and (= "active" status)
         (nil? revoked-at)
         (or (nil? expires-at)
             (.isAfter (.toInstant ^Timestamp expires-at) now))
         (constant-time-equals? (or (row-get row :key_hash) "") provided-hash))))

(defn- query-one
  [datasource sqlvec]
  (first (jdbc/execute! datasource sqlvec {:builder-fn rs/as-unqualified-lower-maps})))

(defn- query-many
  [datasource sqlvec]
  (jdbc/execute! datasource sqlvec {:builder-fn rs/as-unqualified-lower-maps}))

(defn- update-last-used-batch!
  [datasource api-key-ids]
  (when (seq api-key-ids)
    (let [placeholders (str/join ", " (repeat (count api-key-ids) "?::uuid"))
          sql (str "UPDATE dcore_api_keys
                    SET last_used_at = NOW(), updated_at = NOW()
                    WHERE api_key_id IN (" placeholders ")")]
      (jdbc/execute! datasource (into [sql] (map str api-key-ids))))))

(defn- take-pending-batch!
  [pending max-batch-size]
  (let [batch* (volatile! nil)]
    (swap! pending
           (fn [s]
             (let [batch (->> s (take max-batch-size) vec)]
               (vreset! batch* batch)
               (reduce disj s batch))))
    @batch*))

(declare flush-last-used!)

(defn- daemon-thread-factory
  [thread-name]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. runnable (str thread-name "-" (UUID/randomUUID)))
        (.setDaemon true)))))

(defn- new-last-used-tracker
  [{:keys [datasource logger mode flush-interval-ms max-batch-size]
    :or {mode :async
         flush-interval-ms 2000
         max-batch-size 200}}]
  (if (= mode :sync)
    {:mode :sync
     :datasource datasource
     :logger logger}
    (let [pending (atom #{})
          flushing? (atom false)
          scheduler (Executors/newSingleThreadScheduledExecutor
                     (daemon-thread-factory "d-core-api-key-last-used-flush"))
          tracker {:mode :async
                   :datasource datasource
                   :logger logger
                   :pending pending
                   :flushing? flushing?
                   :flush-interval-ms (long flush-interval-ms)
                   :max-batch-size (long max-batch-size)
                   :scheduler scheduler}]
      (.scheduleAtFixedRate ^ScheduledExecutorService scheduler
                            ^Runnable #(flush-last-used! tracker)
                            (long flush-interval-ms)
                            (long flush-interval-ms)
                            TimeUnit/MILLISECONDS)
      tracker)))

(defn- flush-last-used!
  [{:keys [mode datasource logger pending flushing? max-batch-size]}]
  (when (= mode :async)
    (when (compare-and-set! flushing? false true)
      (try
        (loop []
          (let [batch (take-pending-batch! pending max-batch-size)]
            (when (seq batch)
              (let [flushed? (try
                               (update-last-used-batch! datasource batch)
                               true
                               (catch Exception ex
                                 ;; Re-queue batch to avoid dropping updates.
                                 (swap! pending into batch)
                                 (when logger
                                   (logger/log logger :error ::last-used-flush-failed
                                               {:batch-size (count batch)
                                                :error (.getMessage ex)}))
                                 false))]
                (when flushed?
                  (recur))))))
        (finally
          (reset! flushing? false))))))

(defn- record-last-used!
  [tracker api-key-id]
  (when (and tracker api-key-id)
    (case (:mode tracker)
      :sync (update-last-used-batch! (:datasource tracker) [api-key-id])
      :async (do
               (swap! (:pending tracker) conj (str api-key-id))
               (when (>= (count @(:pending tracker)) (:max-batch-size tracker))
                 (future (flush-last-used! tracker))))
      nil)))

(defn- stop-last-used-tracker!
  [tracker]
  (when tracker
    (when (= :async (:mode tracker))
      (flush-last-used! tracker)
      (when-let [^ScheduledExecutorService scheduler (:scheduler tracker)]
        (.shutdown scheduler)
        (.awaitTermination scheduler 2000 TimeUnit/MILLISECONDS)
        (flush-last-used! tracker)))))

(defrecord PostgresApiKeyStore [datasource pepper logger last-used-tracker]
  p/ApiKeyStore
  (ensure-schema! [_ _opts]
    (doseq [ddl ddl-statements]
      (jdbc/execute! datasource [ddl]))
    {:ok true})

  (create-key! [this data _opts]
    (let [tenant-id (tenant-id->str (:tenant-id data))
          _ (when-not (seq tenant-id)
              (throw (ex-info "create-key! requires :tenant-id"
                              {:type ::missing-tenant-id
                               :data data})))
          api-key-id (UUID/randomUUID)
          prefix (generate-prefix)
          secret (generate-secret)
          token (compose-token prefix secret)
          key-hash (secret->hash pepper secret)
          scopes-json (encode-json (vec (sort (normalize-scope-set (:scopes data)))) [])
          limits-json (encode-json (:limits data) {})
          metadata-json (encode-json (:metadata data) {})
          expires-at (->timestamp (:expires-at data))
          row (query-one datasource
                         ["INSERT INTO dcore_api_keys
                           (api_key_id, name, tenant_id, scopes_json, key_prefix, key_hash, status, expires_at, limits_json, metadata_json, created_at, updated_at)
                           VALUES (?::uuid, ?, ?, ?::jsonb, ?, ?, 'active', ?, ?::jsonb, ?::jsonb, NOW(), NOW())
                           RETURNING api_key_id, name, tenant_id, scopes_json, key_prefix, status, expires_at,
                                     last_used_at, limits_json, metadata_json, created_at, updated_at, revoked_at"
                          (str api-key-id)
                          (:name data)
                          tenant-id
                          scopes-json
                          prefix
                          key-hash
                          expires-at
                          limits-json
                          metadata-json])]
      (when logger
        (logger/log logger :info ::api-key-created
                    {:api-key-id (str api-key-id)
                     :tenant-id tenant-id
                     :prefix (mask-prefix prefix)}))
      {:api-key (sanitize-row row)
       :token token}))

  (get-key [_ api-key-id _opts]
    (some-> (query-one datasource
                       ["SELECT api_key_id, name, tenant_id, scopes_json, key_prefix, status, expires_at,
                                last_used_at, limits_json, metadata_json, created_at, updated_at, revoked_at
                         FROM dcore_api_keys
                         WHERE api_key_id = ?::uuid"
                        (str api-key-id)])
            sanitize-row))

  (list-keys [_ filters _opts]
    (let [tenant-id (tenant-id->str (:tenant-id filters))
          status (some-> (:status filters) name str/lower-case)
          limit (some-> (:limit filters) long)
          offset (some-> (:offset filters) long)
          where-parts (cond-> []
                        tenant-id (conj "tenant_id = ?")
                        status (conj "status = ?"))
          params (cond-> []
                   tenant-id (conj tenant-id)
                   status (conj status))
          sql (str "SELECT api_key_id, name, tenant_id, scopes_json, key_prefix, status, expires_at,
                           last_used_at, limits_json, metadata_json, created_at, updated_at, revoked_at
                    FROM dcore_api_keys"
                   (when (seq where-parts)
                     (str " WHERE " (str/join " AND " where-parts)))
                   " ORDER BY created_at DESC"
                   (when (some? limit) " LIMIT ?")
                   (when (some? offset) " OFFSET ?"))
          params (cond-> params
                   (some? limit) (conj limit)
                   (some? offset) (conj offset))]
      (mapv sanitize-row (query-many datasource (into [sql] params)))))

  (revoke-key! [this api-key-id _opts]
    (let [row (query-one datasource
                         ["UPDATE dcore_api_keys
                           SET status = 'revoked', revoked_at = NOW(), updated_at = NOW()
                           WHERE api_key_id = ?::uuid AND status <> 'revoked'
                           RETURNING api_key_id"
                          (str api-key-id)])
          revoked? (boolean row)]
      (when (and logger revoked?)
        (logger/log logger :info ::api-key-revoked {:api-key-id (str api-key-id)}))
      {:revoked? revoked?}))

  (rotate-key! [this api-key-id _opts]
    (let [prefix (generate-prefix)
          secret (generate-secret)
          token (compose-token prefix secret)
          key-hash (secret->hash pepper secret)
          row (query-one datasource
                         ["UPDATE dcore_api_keys
                           SET key_prefix = ?, key_hash = ?, status = 'active', revoked_at = NULL, updated_at = NOW()
                           WHERE api_key_id = ?::uuid
                           RETURNING api_key_id, name, tenant_id, scopes_json, key_prefix, status, expires_at,
                                     last_used_at, limits_json, metadata_json, created_at, updated_at, revoked_at"
                          prefix
                          key-hash
                          (str api-key-id)])]
      (when-not row
        (throw (ex-info "API key not found"
                        {:type ::api-key-not-found
                         :api-key-id (str api-key-id)})))
      (when logger
        (logger/log logger :info ::api-key-rotated
                    {:api-key-id (str api-key-id)
                     :prefix (mask-prefix prefix)}))
      {:api-key (sanitize-row row)
       :token token}))

  (authenticate-key [_ token _opts]
    (when-let [{:keys [prefix secret]} (parse-token token)]
      (let [row (query-one datasource
                           ["SELECT api_key_id, name, tenant_id, scopes_json, key_prefix, key_hash, status, expires_at,
                                    last_used_at, limits_json, metadata_json, created_at, updated_at, revoked_at
                             FROM dcore_api_keys
                             WHERE key_prefix = ?
                             LIMIT 1"
                            prefix])
            provided-hash (secret->hash pepper secret)]
        (when (and row (auth-valid? row provided-hash))
          (record-last-used! last-used-tracker (row-get row :api_key_id))
          (sanitize-row row)))))
  )

(defmethod ig/init-key :d-core.core.api-keys.postgres/store
  [_ {:keys [postgres-client pepper logger bootstrap-schema? last-used-update]
      :or {pepper ""
           bootstrap-schema? false}}]
  (when-not postgres-client
    (throw (ex-info "API key Postgres store requires :postgres-client"
                    {:type ::missing-postgres-client})))
  (let [datasource (:datasource postgres-client)
        last-used-tracker (new-last-used-tracker
                           (merge {:datasource datasource
                                   :logger logger}
                                  (or last-used-update {})))
        store (->PostgresApiKeyStore datasource pepper logger last-used-tracker)]
    (when bootstrap-schema?
      (p/ensure-schema! store {}))
    store))

(defmethod ig/halt-key! :d-core.core.api-keys.postgres/store
  [_ store]
  (stop-last-used-tracker! (:last-used-tracker store)))
