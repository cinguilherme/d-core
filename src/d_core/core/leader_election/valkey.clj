(ns d-core.core.leader-election.valkey
  (:require [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.logics.redis-like :as redis-logics]
            [d-core.core.leader-election.observability :as obs]
            [d-core.core.leader-election.redis-common :as redis-common]
            [d-core.core.leader-election.protocol :as p]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn eval-acquire!
  [valkey-client lease-key fencing-key owner-id token now-ms lease-ms]
  (car/wcar (:conn valkey-client)
            (car/eval redis-common/acquire-lua
                      2
                      lease-key
                      fencing-key
                      owner-id
                      token
                      (str now-ms)
                      (str lease-ms))))

(defn eval-renew!
  [valkey-client lease-key token now-ms lease-ms]
  (car/wcar (:conn valkey-client)
            (car/eval redis-common/renew-lua
                      1
                      lease-key
                      token
                      (str now-ms)
                      (str lease-ms))))

(defn eval-resign!
  [valkey-client lease-key token]
  (car/wcar (:conn valkey-client)
            (car/eval redis-common/resign-lua
                      1
                      lease-key
                      token)))

(defn eval-status
  [valkey-client lease-key]
  (car/wcar (:conn valkey-client)
            (car/eval redis-common/status-lua
                      1
                      lease-key)))

(defrecord ValkeyLeaderElection [valkey-client owner-id prefix default-lease-ms clock observability]
  p/LeaderElectionProtocol
  (acquire! [_ election-id opts]
    (obs/observe-operation observability :valkey :acquire election-id
                           (fn []
                             (redis-logics/acquire! {:backend :valkey
                                                     :client valkey-client
                                                     :owner-id owner-id
                                                     :prefix prefix
                                                     :default-lease-ms default-lease-ms
                                                     :clock clock
                                                     :eval-acquire! eval-acquire!}
                                                    election-id
                                                    opts))))

  (renew! [_ election-id token opts]
    (obs/observe-operation observability :valkey :renew election-id
                           (fn []
                             (redis-logics/renew! {:backend :valkey
                                                   :client valkey-client
                                                   :prefix prefix
                                                   :default-lease-ms default-lease-ms
                                                   :clock clock
                                                   :eval-renew! eval-renew!}
                                                  election-id
                                                  token
                                                  opts))))

  (resign! [_ election-id token _opts]
    (obs/observe-operation observability :valkey :resign election-id
                           (fn []
                             (redis-logics/resign! {:backend :valkey
                                                    :client valkey-client
                                                    :prefix prefix
                                                    :eval-resign! eval-resign!}
                                                   election-id
                                                   token))))

  (status [_ election-id _opts]
    (obs/observe-operation observability :valkey :status election-id
                           (fn []
                             (redis-logics/status {:backend :valkey
                                                   :client valkey-client
                                                   :prefix prefix
                                                   :eval-status eval-status}
                                                  election-id)))))

(defmethod ig/init-key :d-core.core.leader-election.valkey/valkey
  [_ {:keys [valkey-client owner-id prefix default-lease-ms clock logger metrics]
      :or {prefix redis-common/default-prefix
           default-lease-ms common/default-lease-ms}}]
  (when-not valkey-client
    (throw (ex-info "Valkey leader election requires :valkey-client"
                    {:type ::missing-valkey-client})))
  (->ValkeyLeaderElection valkey-client
                          (common/normalize-owner-id owner-id)
                          (redis-common/normalize-prefix prefix)
                          (common/require-positive-long default-lease-ms :default-lease-ms)
                          (common/normalize-clock clock)
                          (obs/make-context logger metrics)))
