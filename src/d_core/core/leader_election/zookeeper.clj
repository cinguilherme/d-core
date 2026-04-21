(ns d-core.core.leader-election.zookeeper
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.core.clients.zookeeper.client :as zk-client]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.logics.zookeeper :as zk-logics]
            [d-core.core.leader-election.observability :as obs]
            [d-core.core.leader-election.protocol :as p]
            [integrant.core :as ig])
  (:import [java.security MessageDigest]
           [org.apache.zookeeper CreateMode KeeperException$NoNodeException KeeperException$NodeExistsException]))

(def default-base-path
  "/dcore/leader-election")

(def ^:private backend
  :zookeeper)

(def ^:private candidate-prefix
  "candidate-")

(def ^:private segment-hash-length
  12)

(def ^:private max-path-segment-length
  96)

(defn normalize-base-path
  [value]
  (let [path (str/trim (str (or value default-base-path)))]
    (when-not (re-matches #"^/[^/\u0000]+(?:/[^/\u0000]+)*$" path)
      (throw (ex-info "ZooKeeper leader election :base-path must be an absolute slash-delimited path"
                      {:type ::invalid-base-path
                       :field :base-path
                       :value value})))
    path))

(defn- sha256-hex
  [value]
  (let [digest (MessageDigest/getInstance "SHA-256")
        bytes (.digest digest (.getBytes (str value) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and % 0xff)) bytes))))

(defn election-path-segment
  [election-id]
  (let [normalized-id (common/normalize-election-id election-id)
        safe-id (-> normalized-id
                    str/lower-case
                    (str/replace #"[^a-z0-9-]+" "-")
                    (str/replace #"-+" "-")
                    (str/replace #"^-+" "")
                    (str/replace #"-+$" ""))
        hash-part (subs (sha256-hex normalized-id) 0 segment-hash-length)
        max-safe-id-len (max 0 (- max-path-segment-length (count hash-part) 1))
        safe-id (subs (or safe-id "") 0 (min (count safe-id) max-safe-id-len))]
    (if (seq safe-id)
      (str safe-id "-" hash-part)
      (str "election-" hash-part))))

(defn election-path
  [base-path election-id]
  (str (normalize-base-path base-path) "/" (election-path-segment election-id)))

(defn- candidate-path-prefix
  [election-path]
  (str election-path "/" candidate-prefix))

(defn- basename
  [path]
  (last (str/split (str path) #"/")))

(defn- parse-sequence-suffix
  [candidate-name]
  (some->> (re-find #"(\d+)$" (str candidate-name))
           second
           common/parse-long-safe))

(defn- candidate-sort-key
  [candidate-name]
  [(or (parse-sequence-suffix candidate-name) Long/MAX_VALUE)
   (str candidate-name)])

(defn- sort-candidates
  [children]
  (sort-by candidate-sort-key children))

(defn- candidate-payload
  [owner-id token election-id created-at-ms]
  (.getBytes (json/generate-string {:owner-id owner-id
                                    :token token
                                    :election-id election-id
                                    :created-at-ms created-at-ms})
             "UTF-8"))

(defn- parse-candidate-data
  [data]
  (when (some? data)
    (try
      (let [parsed (json/parse-string (String. ^bytes data "UTF-8") true)]
        (cond-> {}
          (string? (:owner-id parsed)) (assoc :owner-id (:owner-id parsed))
          (string? (:token parsed)) (assoc :token (:token parsed))
          (string? (:election-id parsed)) (assoc :election-id (:election-id parsed))
          (integer? (:created-at-ms parsed)) (assoc :created-at-ms (long (:created-at-ms parsed)))))
      (catch Exception _
        nil))))

(defn- provider-error
  [message data cause]
  (throw (ex-info message
                  (merge {:type ::provider-error
                          :backend backend}
                         data)
                  cause)))

(defn ensure-path!
  [client path]
  (try
    (.. (:curator client) create creatingParentsIfNeeded (withMode CreateMode/PERSISTENT) (forPath path))
    path
    (catch KeeperException$NodeExistsException _
      path)
    (catch Exception ex
      (provider-error "ZooKeeper failed to ensure path"
                      {:kind :ensure-path
                       :path path}
                      ex))))

(defn create-candidate!
  [client election-path payload]
  (try
    (.. (:curator client) create creatingParentsIfNeeded (withMode CreateMode/EPHEMERAL_SEQUENTIAL) (forPath (candidate-path-prefix election-path) payload))
    (catch Exception ex
      (provider-error "ZooKeeper failed to create leader-election candidate"
                      {:kind :create-candidate
                       :path election-path}
                      ex))))

(defn list-children
  [client path]
  (try
    (vec (.. (:curator client) getChildren (forPath path)))
    (catch KeeperException$NoNodeException _
      nil)
    (catch Exception ex
      (provider-error "ZooKeeper failed to list leader-election candidates"
                      {:kind :list-children
                       :path path}
                      ex))))

(defn read-node-data
  [client path]
  (try
    (.. (:curator client) getData (forPath path))
    (catch KeeperException$NoNodeException _
      nil)
    (catch Exception ex
      (provider-error "ZooKeeper failed to read leader-election candidate data"
                      {:kind :read-node-data
                       :path path}
                      ex))))

(defn delete-node!
  [client path]
  (try
    (.. (:curator client) delete (forPath path))
    true
    (catch KeeperException$NoNodeException _
      false)
    (catch Exception ex
      (provider-error "ZooKeeper failed to delete leader-election candidate"
                      {:kind :delete-node
                       :path path}
                      ex))))

(defn- leader-info
  [client election-path]
  (loop [attempt 0]
    (let [children (sort-candidates (or (list-children client election-path) []))]
      (when-let [leader-name (first children)]
        (let [leader-path (str election-path "/" leader-name)
              leader-data (read-node-data client leader-path)]
          (if (and (nil? leader-data) (zero? attempt))
            (recur (inc attempt))
            (merge {:name leader-name
                    :path leader-path
                    :fencing (parse-sequence-suffix leader-name)}
                   (select-keys (or (parse-candidate-data leader-data) {}) [:owner-id]))))))))

(defn- result-parts
  [status leader]
  [(name status)
   (:owner-id leader)
   (some-> (:fencing leader) str)])

(defn- connected-state!
  [client op election-id]
  (when-not (zk-client/safe-state? client)
    (throw (ex-info "ZooKeeper leader election requires a connected client"
                    {:type ::not-connected
                     :backend backend
                     :kind :connection
                     :op op
                     :election-id election-id
                     :connection-state (zk-client/current-state client)}))))

(defn- compatible-lease-ms
  [opts default-lease-ms]
  (if (contains? opts :lease-ms)
    (let [lease-ms (common/require-positive-long (:lease-ms opts) :lease-ms)]
      (when-not (= lease-ms default-lease-ms)
        (throw (ex-info "ZooKeeper leader election is session-backed and does not support per-call :lease-ms overrides"
                        {:type ::unsupported-lease-ms
                         :field :lease-ms
                         :lease-ms lease-ms
                         :default-lease-ms default-lease-ms})))
      lease-ms)
    default-lease-ms))

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

(defrecord ZooKeeperLeaderElection [zookeeper-client owner-id base-path default-lease-ms clock observability ownership]
  p/LeaderElectionProtocol
  (acquire! [_ election-id opts]
    (obs/observe-operation observability backend :acquire election-id
                           (fn []
                             (let [election-id (common/normalize-election-id election-id)
                                   _lease-ms (compatible-lease-ms opts default-lease-ms)
                                   _ (connected-state! zookeeper-client :acquire election-id)
                                   election-path (election-path base-path election-id)
                                   token (common/generate-token)
                                   candidate-path (create-candidate! zookeeper-client
                                                                     election-path
                                                                     (candidate-payload owner-id token election-id (common/now-ms clock)))
                                   own-name (basename candidate-path)
                                   leader (leader-info zookeeper-client election-path)]
                               (if (= own-name (:name leader))
                                 (do
                                   (assoc-local-record! ownership election-id {:token token
                                                                               :path candidate-path
                                                                               :fencing (:fencing leader)})
                                   (common/acquire-result backend election-id
                                                          ["acquired"
                                                           owner-id
                                                           (some-> (:fencing leader) str)
                                                           token]))
                                 (do
                                   (delete-node! zookeeper-client candidate-path)
                                   (clear-local-record! ownership election-id)
                                   (common/acquire-result backend election-id
                                                          (result-parts :busy leader))))))))

  (renew! [_ election-id token opts]
    (obs/observe-operation observability backend :renew election-id
                           (fn []
                             (let [election-id (common/normalize-election-id election-id)
                                   token (common/normalize-token token)
                                   _lease-ms (compatible-lease-ms opts default-lease-ms)]
                               (if-not (zk-client/safe-state? zookeeper-client)
                                 (do
                                   (clear-local-record! ownership election-id)
                                   (common/renew-result backend election-id ["lost"]))
                                 (let [record (local-record ownership election-id)
                                       election-path (election-path base-path election-id)
                                       leader (leader-info zookeeper-client election-path)]
                                   (if (and record
                                            (= token (:token record))
                                            (= (basename (:path record)) (:name leader)))
                                     (common/renew-result backend election-id
                                                          ["renewed"
                                                           owner-id
                                                           (some-> (:fencing leader) str)
                                                           token])
                                     (do
                                       (when (and record
                                                  (= token (:token record)))
                                         (clear-local-record! ownership election-id))
                                       (common/renew-result backend election-id
                                                            (result-parts :lost leader))))))))))

  (resign! [_ election-id token _opts]
    (obs/observe-operation observability backend :resign election-id
                           (fn []
                             (let [election-id (common/normalize-election-id election-id)
                                   token (common/normalize-token token)]
                               (if-not (zk-client/safe-state? zookeeper-client)
                                 (do
                                   (clear-local-record! ownership election-id)
                                   (common/resign-result backend election-id ["not-owner"]))
                                 (let [record (local-record ownership election-id)
                                       election-path (election-path base-path election-id)
                                       leader (leader-info zookeeper-client election-path)]
                                   (if (and record
                                            (= token (:token record))
                                            (= (basename (:path record)) (:name leader)))
                                     (if (delete-node! zookeeper-client (:path record))
                                       (do
                                         (clear-local-record! ownership election-id)
                                         (common/resign-result backend election-id
                                                               ["released"
                                                                owner-id
                                                                (some-> (:fencing leader) str)]))
                                       (do
                                         (clear-local-record! ownership election-id)
                                         (common/resign-result backend election-id
                                                               (result-parts :not-owner leader))))
                                     (do
                                       (when (and record
                                                  (= token (:token record)))
                                         (clear-local-record! ownership election-id))
                                       (common/resign-result backend election-id
                                                             (result-parts :not-owner leader))))))))))

  (status [_ election-id _opts]
    (obs/observe-operation observability backend :status election-id
                           (fn []
                             (let [election-id (common/normalize-election-id election-id)
                                   _ (connected-state! zookeeper-client :status election-id)
                                   leader (leader-info zookeeper-client (election-path base-path election-id))]
                               (if leader
                                 (common/status-result backend election-id (result-parts :held leader))
                                 (common/status-result backend election-id ["vacant"])))))))

(defmethod ig/init-key :d-core.core.leader-election.zookeeper/zookeeper
  [_ {:keys [zookeeper-client owner-id base-path default-lease-ms clock logger metrics] :as opts}]
  (when-not zookeeper-client
    (throw (ex-info "ZooKeeper leader election requires :zookeeper-client"
                    {:type ::missing-zookeeper-client})))
  (let [session-timeout-ms (:session-timeout-ms zookeeper-client)
        _ (when (zk-logics/missing-session-timeout? session-timeout-ms)
            (throw (ex-info "ZooKeeper leader election requires a client with :session-timeout-ms"
                            {:type ::missing-session-timeout-ms})))
        {:keys [default-lease-ms explicit-default?]}
        (zk-logics/resolve-default-lease-ms opts session-timeout-ms)
        base-path (normalize-base-path base-path)]
    (when (zk-logics/default-lease-ms-mismatch? explicit-default? default-lease-ms session-timeout-ms)
      (throw (ex-info "ZooKeeper leader election :default-lease-ms must match the ZooKeeper client session timeout"
                      {:type ::default-lease-ms-mismatch
                       :field :default-lease-ms
                       :default-lease-ms default-lease-ms
                       :session-timeout-ms session-timeout-ms})))
    (connected-state! zookeeper-client :init nil)
    (ensure-path! zookeeper-client base-path)
    (->ZooKeeperLeaderElection zookeeper-client
                               (common/normalize-owner-id owner-id)
                               base-path
                               default-lease-ms
                               (common/normalize-clock clock)
                               (obs/make-context logger metrics)
                               (atom {}))))
