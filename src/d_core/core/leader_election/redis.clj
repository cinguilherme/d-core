(ns d-core.core.leader-election.redis
  (:require [d-core.core.clients.redis.utils :as redis-utils]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.logics.redis-like :as redis-logics]
            [d-core.core.leader-election.observability :as obs]
            [d-core.core.leader-election.redis-common :as redis-common]
            [d-core.core.leader-election.protocol :as p]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn eval-acquire!
  [redis-client lease-key fencing-key owner-id token now-ms lease-ms]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval redis-common/acquire-lua
                      2
                      lease-key
                      fencing-key
                      owner-id
                      token
                      (str now-ms)
                      (str lease-ms))))

(defn eval-renew!
  [redis-client lease-key token now-ms lease-ms]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval redis-common/renew-lua
                      1
                      lease-key
                      token
                      (str now-ms)
                      (str lease-ms))))

(defn eval-resign!
  [redis-client lease-key token]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval redis-common/resign-lua
                      1
                      lease-key
                      token)))

(defn eval-status
  [redis-client lease-key]
  (car/wcar (redis-utils/conn redis-client)
            (car/eval redis-common/status-lua
                      1
                      lease-key)))

(defrecord RedisLeaderElection [redis-client owner-id prefix default-lease-ms clock observability]
  p/LeaderElectionProtocol
  (acquire! [_ election-id opts]
    (obs/observe-operation observability :redis :acquire election-id
                           (fn []
                             (redis-logics/acquire! {:backend :redis
                                                     :client redis-client
                                                     :owner-id owner-id
                                                     :prefix prefix
                                                     :default-lease-ms default-lease-ms
                                                     :clock clock
                                                     :eval-acquire! eval-acquire!}
                                                    election-id
                                                    opts))))

  (renew! [_ election-id token opts]
    (obs/observe-operation observability :redis :renew election-id
                           (fn []
                             (redis-logics/renew! {:backend :redis
                                                   :client redis-client
                                                   :prefix prefix
                                                   :default-lease-ms default-lease-ms
                                                   :clock clock
                                                   :eval-renew! eval-renew!}
                                                  election-id
                                                  token
                                                  opts))))

  (resign! [_ election-id token _opts]
    (obs/observe-operation observability :redis :resign election-id
                           (fn []
                             (redis-logics/resign! {:backend :redis
                                                    :client redis-client
                                                    :prefix prefix
                                                    :eval-resign! eval-resign!}
                                                   election-id
                                                   token))))

  (status [_ election-id _opts]
    (obs/observe-operation observability :redis :status election-id
                           (fn []
                             (redis-logics/status {:backend :redis
                                                   :client redis-client
                                                   :prefix prefix
                                                   :eval-status eval-status}
                                                  election-id)))))

(defmethod ig/init-key :d-core.core.leader-election.redis/redis
  [_ {:keys [redis-client owner-id prefix default-lease-ms clock logger metrics]
      :or {prefix redis-common/default-prefix
           default-lease-ms common/default-lease-ms}}]
  (when-not redis-client
    (throw (ex-info "Redis leader election requires :redis-client"
                    {:type ::missing-redis-client})))
  (->RedisLeaderElection redis-client
                         (common/normalize-owner-id owner-id)
                         (redis-common/normalize-prefix prefix)
                         (common/require-positive-long default-lease-ms :default-lease-ms)
                         (common/normalize-clock clock)
                         (obs/make-context logger metrics)))
