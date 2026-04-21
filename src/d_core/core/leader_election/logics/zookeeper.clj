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
