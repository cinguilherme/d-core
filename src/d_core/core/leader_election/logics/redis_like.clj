(ns d-core.core.leader-election.logics.redis-like
  (:require [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.redis-common :as redis-common]))

(defn acquire!
  [{:keys [backend client owner-id prefix default-lease-ms clock eval-acquire!]} election-id opts]
  (let [election-id (common/normalize-election-id election-id)
        token (common/generate-token)
        now-ms (common/now-ms clock)
        lease-ms (common/lease-ms opts default-lease-ms)
        response (eval-acquire! client
                                (redis-common/lease-key prefix election-id)
                                (redis-common/fencing-key prefix election-id)
                                owner-id
                                token
                                now-ms
                                lease-ms)]
    (common/acquire-result backend election-id response)))

(defn renew!
  [{:keys [backend client prefix default-lease-ms clock eval-renew!]} election-id token opts]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)
        now-ms (common/now-ms clock)
        lease-ms (common/lease-ms opts default-lease-ms)
        response (eval-renew! client
                              (redis-common/lease-key prefix election-id)
                              token
                              now-ms
                              lease-ms)]
    (common/renew-result backend election-id response)))

(defn resign!
  [{:keys [backend client prefix eval-resign!]} election-id token]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)
        response (eval-resign! client
                               (redis-common/lease-key prefix election-id)
                               token)]
    (common/resign-result backend election-id response)))

(defn status
  [{:keys [backend client prefix eval-status]} election-id]
  (let [election-id (common/normalize-election-id election-id)
        response (eval-status client
                              (redis-common/lease-key prefix election-id))]
    (common/status-result backend election-id response)))
