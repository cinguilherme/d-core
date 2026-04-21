(ns d-core.core.leader-election.logics.kubernetes-lease-test
  (:require [clojure.test :refer [deftest is]]
            [d-core.core.leader-election.logics.kubernetes-lease :as k8s-logics]))

(def ^:private fixed-now-ms
  1700000000000)

(defn- take-next!
  [state]
  (let [value (first @state)]
    (swap! state rest)
    value))

(defn- lease-active?
  [lease now-ms]
  (and (:holder-identity lease)
       (:expires-at-ms lease)
       (< now-ms (:expires-at-ms lease))))

(defn- lease-owned-by-token?
  [lease token now-ms]
  (and (lease-active? lease now-ms)
       (= token (:token lease))))

(defn- lease-owned-by-caller?
  [lease owner-id token now-ms]
  (and (lease-owned-by-token? lease token now-ms)
       (= owner-id (:holder-identity lease))))

(defn- remaining-ttl-ms
  [lease now-ms]
  (when (:expires-at-ms lease)
    (max 0 (- (:expires-at-ms lease) now-ms))))

(deftest renew-retries-on-conflict-when-caller-still-owner
  (let [get-lease-queue (atom [{:holder-identity "node-a"
                                :token "token-1"
                                :lease-transitions 5
                                :expires-at-ms (+ fixed-now-ms 5000)}
                               {:holder-identity "node-a"
                                :token "token-1"
                                :lease-transitions 5
                                :expires-at-ms (+ fixed-now-ms 5000)}])
        replace-queue (atom [::conflict
                             {:holder-identity "node-a"
                              :token "token-1"
                              :lease-transitions 5
                              :expires-at-ms (+ fixed-now-ms 9000)}])
        replace-calls (atom [])
        ctx {:backend :kubernetes-lease
             :owner-id "node-a"
             :lease-name-prefix "dcore-leader-"
             :default-lease-ms 15000
             :clock (constantly fixed-now-ms)
             :conflict-marker ::conflict
             :lease-name (fn [_ election-id] (str "lease-" election-id))
             :get-lease (fn [_lease-name] (take-next! get-lease-queue))
             :replace-lease! (fn [lease-name payload]
                               (swap! replace-calls conj {:lease-name lease-name
                                                          :payload payload})
                               (take-next! replace-queue))
             :lease-active? lease-active?
             :lease-owned-by-token? lease-owned-by-token?
             :lease-owned-by-caller? lease-owned-by-caller?
             :renew-lease-body (fn [lease-name lease owner-id token election-id now-ms lease-ms]
                                 {:lease-name lease-name
                                  :lease lease
                                  :owner-id owner-id
                                  :token token
                                  :election-id election-id
                                  :now-ms now-ms
                                  :lease-ms lease-ms})
             :remaining-ttl-ms remaining-ttl-ms}]
    (is (= {:ok true
            :status :renewed
            :backend :kubernetes-lease
            :election-id "orders"
            :owner-id "node-a"
            :fencing 5
            :remaining-ttl-ms 9000
            :token "token-1"}
           (k8s-logics/renew! ctx "orders" "token-1" {:lease-ms 9000})))
    (is (= 2 (count @replace-calls)))))
