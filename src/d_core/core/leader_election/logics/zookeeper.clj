(ns d-core.core.leader-election.logics.zookeeper
  (:require [d-core.core.leader-election.common :as common]))

(defn missing-session-timeout?
  [session-timeout-ms]
  (not session-timeout-ms))

(defn resolve-default-lease-ms
  [{:keys [default-lease-ms] :as opts} session-timeout-ms]
  (let [explicit-default? (contains? opts :default-lease-ms)
        default-lease-ms (if explicit-default?
                           (common/require-positive-long default-lease-ms :default-lease-ms)
                           session-timeout-ms)]
    {:default-lease-ms default-lease-ms
     :explicit-default? explicit-default?}))

(defn default-lease-ms-mismatch?
  [explicit-default? default-lease-ms session-timeout-ms]
  (and explicit-default?
       (not= default-lease-ms session-timeout-ms)))

(defn acquire!
  [{:keys [backend zookeeper-client owner-id base-path default-lease-ms clock ownership
           election-path compatible-lease-ms connected-state!
           create-candidate! candidate-payload basename leader-info
           assoc-local-record! delete-node! clear-local-record! result-parts]}
   election-id
   opts]
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
                               (result-parts :busy leader))))))

(defn renew!
  [{:keys [backend zookeeper-client owner-id base-path default-lease-ms ownership
           election-path compatible-lease-ms safe-state?
           local-record basename leader-info clear-local-record! result-parts]}
   election-id
   token
   opts]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)
        _lease-ms (compatible-lease-ms opts default-lease-ms)]
    (if-not (safe-state? zookeeper-client)
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
                                 (result-parts :lost leader))))))))

(defn resign!
  [{:keys [backend zookeeper-client owner-id base-path ownership
           election-path safe-state? local-record basename leader-info
           clear-local-record! delete-node! result-parts]}
   election-id
   token]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)]
    (if-not (safe-state? zookeeper-client)
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
                                  (result-parts :not-owner leader))))))))

(defn status
  [{:keys [backend zookeeper-client base-path election-path connected-state! leader-info result-parts]}
   election-id]
  (let [election-id (common/normalize-election-id election-id)
        _ (connected-state! zookeeper-client :status election-id)
        leader (leader-info zookeeper-client (election-path base-path election-id))]
    (if leader
      (common/status-result backend election-id (result-parts :held leader))
      (common/status-result backend election-id ["vacant"]))))
