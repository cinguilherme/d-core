(ns d-core.core.leader-election.kubernetes-lease-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.kubernetes.client :as kube]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.kubernetes-lease :as k8s]
            [d-core.core.leader-election.protocol :as p]
            [integrant.core :as ig])
  (:import (java.time Instant)))

(def ^:private fixed-now-ms
  1700000000000)

(defn- iso-ms
  [ms]
  (.toString (Instant/ofEpochMilli ms)))

(defn- make-component
  []
  (k8s/->KubernetesLeaseLeaderElection {:namespace "workers"}
                                       "node-a"
                                       "workers"
                                       (k8s/normalize-lease-name-prefix nil)
                                       15000
                                       (constantly fixed-now-ms)))

(defn- lease-response
  [{:keys [name namespace owner-id token renew-time-ms acquire-time-ms lease-duration-seconds lease-transitions resource-version labels annotations]
    :or {name "dcore-leader-orders-abc123456789"
         namespace "workers"
         resource-version "11"
         lease-duration-seconds 15
         lease-transitions 1}}]
  {:status 200
   :body {:apiVersion "coordination.k8s.io/v1"
          :kind "Lease"
          :metadata (cond-> {:name name
                             :namespace namespace
                             :resourceVersion resource-version
                             :annotations (merge {"dcore.io/leader-election-id" "orders"}
                                                 annotations
                                                 (when token
                                                   {"dcore.io/leader-election-token" token}))}
                      labels (assoc :labels labels))
          :spec (cond-> {:leaseTransitions lease-transitions}
                  owner-id (assoc :holderIdentity owner-id)
                  acquire-time-ms (assoc :acquireTime (iso-ms acquire-time-ms))
                  renew-time-ms (assoc :renewTime (iso-ms renew-time-ms))
                  lease-duration-seconds (assoc :leaseDurationSeconds lease-duration-seconds))}})

(defn- not-found-response
  [lease-name]
  {:status 404
   :body {:kind "Status"
          :reason "NotFound"
          :details {:name lease-name}}})

(deftest lease-name-normalization
  (let [name-a (k8s/lease-name "DCore_Leader" "Orders/Sync:Primary")
        name-b (k8s/lease-name "DCore_Leader" "Orders/Sync:Primary")]
    (is (= name-a name-b))
    (is (re-matches #"[a-z0-9-]+" name-a))
    (is (<= (count name-a) 63))))

(deftest acquire-on-missing-lease-creates
  (let [component (make-component)
        lease-name (k8s/lease-name "dcore-leader-" "orders")
        requests (atom [])]
    (with-redefs [common/generate-token (fn [] "token-1")
                  kube/request! (fn [_ request]
                                  (swap! requests conj request)
                                  (cond
                                    (= [:get (str "/apis/coordination.k8s.io/v1/namespaces/workers/leases/" lease-name)]
                                       [(:method request) (:path request)])
                                    (not-found-response lease-name)

                                    (= [:post "/apis/coordination.k8s.io/v1/namespaces/workers/leases"]
                                       [(:method request) (:path request)])
                                    (lease-response {:name lease-name
                                                     :owner-id "node-a"
                                                     :token "token-1"
                                                     :resource-version "1"
                                                     :lease-transitions 1
                                                     :renew-time-ms fixed-now-ms
                                                     :acquire-time-ms fixed-now-ms
                                    :lease-duration-seconds 5})

                                    :else
                                    (throw (ex-info "Unexpected Kubernetes request"
                                                    {:request request}))))]
      (let [result (p/acquire! component :orders {:lease-ms 5000})
            body (json/parse-string (:body (second @requests)) true)]
        (is (= {:ok true
                :status :acquired
                :backend :kubernetes-lease
                :election-id "orders"
                :owner-id "node-a"
                :fencing 1
                :remaining-ttl-ms 5000
                :token "token-1"}
               result))
        (is (= 1 (get-in body [:spec :leaseTransitions])))
        (is (= "token-1" (get-in body [:metadata :annotations :dcore.io/leader-election-token])))
        (is (= "orders" (get-in body [:metadata :annotations :dcore.io/leader-election-id])))))))

(deftest acquire-on-active-lease-returns-busy
  (let [component (make-component)
        lease-name (k8s/lease-name "dcore-leader-" "orders")]
    (with-redefs [common/generate-token (fn [] "token-1")
                  kube/request! (fn [_ _]
                                  (lease-response {:name lease-name
                                                   :owner-id "node-b"
                                                   :token "token-b"
                                                   :lease-transitions 4
                                                   :renew-time-ms fixed-now-ms
                                                   :acquire-time-ms (- fixed-now-ms 1000)
                                                   :lease-duration-seconds 10}))]
      (is (= {:ok true
              :status :busy
              :backend :kubernetes-lease
              :election-id "orders"
              :owner-id "node-b"
              :fencing 4
              :remaining-ttl-ms 10000}
             (p/acquire! component "orders" {:lease-ms 5000})))))) 

(deftest acquire-on-expired-lease-replaces-and-increments-fencing
  (let [component (make-component)
        lease-name (k8s/lease-name "dcore-leader-" "orders")
        calls (atom [])]
    (with-redefs [common/generate-token (fn [] "token-2")
                  kube/request! (fn [_ request]
                                  (swap! calls conj request)
                                  (case (:method request)
                                    :get (lease-response {:name lease-name
                                                         :owner-id "node-b"
                                                         :token "token-b"
                                                         :resource-version "9"
                                                         :lease-transitions 3
                                                         :renew-time-ms (- fixed-now-ms 10000)
                                                         :acquire-time-ms (- fixed-now-ms 12000)
                                                         :lease-duration-seconds 2})
                                    :put (lease-response {:name lease-name
                                                         :owner-id "node-a"
                                                         :token "token-2"
                                                         :resource-version "10"
                                                         :lease-transitions 4
                                                         :renew-time-ms fixed-now-ms
                                                         :acquire-time-ms fixed-now-ms
                                                         :lease-duration-seconds 3})))]
      (let [result (p/acquire! component "orders" {:lease-ms 2500})
            body (json/parse-string (:body (second @calls)) true)]
        (is (= :acquired (:status result)))
        (is (= 4 (:fencing result)))
        (is (= 3 (get-in body [:spec :leaseDurationSeconds])))
        (is (= 4 (get-in body [:spec :leaseTransitions])))))))

(deftest renew-contracts
  (let [component (make-component)
        lease-name (k8s/lease-name "dcore-leader-" "orders")
        calls (atom [])]
    (testing "renew succeeds with matching token"
      (with-redefs [kube/request! (fn [_ request]
                                    (swap! calls conj request)
                                    (case (:method request)
                                      :get (lease-response {:name lease-name
                                                           :owner-id "node-a"
                                                           :token "token-1"
                                                           :resource-version "7"
                                                           :lease-transitions 5
                                                           :renew-time-ms fixed-now-ms
                                                           :acquire-time-ms (- fixed-now-ms 1000)
                                                           :lease-duration-seconds 20})
                                      :put (lease-response {:name lease-name
                                                           :owner-id "node-a"
                                                           :token "token-1"
                                                           :resource-version "8"
                                                           :lease-transitions 5
                                                           :renew-time-ms fixed-now-ms
                                                           :acquire-time-ms (- fixed-now-ms 1000)
                                                           :lease-duration-seconds 9})))]
        (is (= {:ok true
                :status :renewed
                :backend :kubernetes-lease
                :election-id "orders"
                :owner-id "node-a"
                :fencing 5
                :remaining-ttl-ms 9000
                :token "token-1"}
               (p/renew! component :orders "token-1" {:lease-ms 9000})))
        (is (= 5 (get-in (json/parse-string (:body (second @calls)) true)
                         [:spec :leaseTransitions])))))

    (testing "renew returns lost for wrong token"
      (with-redefs [kube/request! (fn [_ _]
                                    (lease-response {:name lease-name
                                                     :owner-id "node-b"
                                                     :token "token-b"
                                                     :lease-transitions 6
                                                     :renew-time-ms fixed-now-ms
                                                     :acquire-time-ms (- fixed-now-ms 1000)
                                                     :lease-duration-seconds 8}))]
        (is (= {:ok true
                :status :lost
                :backend :kubernetes-lease
                :election-id "orders"
                :owner-id "node-b"
                :fencing 6
                :remaining-ttl-ms 8000}
               (p/renew! component "orders" "wrong-token" nil)))))

    (testing "renew returns plain lost after expiry"
      (with-redefs [kube/request! (fn [_ _]
                                    (lease-response {:name lease-name
                                                     :owner-id "node-a"
                                                     :token "token-1"
                                                     :lease-transitions 6
                                                     :renew-time-ms (- fixed-now-ms 6000)
                                                     :acquire-time-ms (- fixed-now-ms 7000)
                                                     :lease-duration-seconds 2}))]
        (is (= {:ok true
                :status :lost
                :backend :kubernetes-lease
                :election-id "orders"}
               (p/renew! component "orders" "token-1" nil)))))))

(deftest resign-and-status-contracts
  (let [component (make-component)
        lease-name (k8s/lease-name "dcore-leader-" "orders")]
    (testing "resign succeeds"
      (with-redefs [kube/request! (fn [_ request]
                                    (case (:method request)
                                      :get (lease-response {:name lease-name
                                                           :owner-id "node-a"
                                                           :token "token-1"
                                                           :resource-version "3"
                                                           :lease-transitions 7
                                                           :renew-time-ms fixed-now-ms
                                                           :acquire-time-ms (- fixed-now-ms 500)
                                                           :lease-duration-seconds 6})
                                      :put (lease-response {:name lease-name
                                                           :resource-version "4"
                                                           :lease-transitions 7
                                                           :lease-duration-seconds nil})))]
        (is (= {:ok true
                :status :released
                :backend :kubernetes-lease
                :election-id "orders"
                :owner-id "node-a"
                :fencing 7}
               (p/resign! component :orders "token-1" nil)))))

    (testing "resign returns not-owner for foreign holder"
      (with-redefs [kube/request! (fn [_ _]
                                    (lease-response {:name lease-name
                                                     :owner-id "node-b"
                                                     :token "token-b"
                                                     :lease-transitions 8
                                                     :renew-time-ms fixed-now-ms
                                                     :acquire-time-ms (- fixed-now-ms 500)
                                                     :lease-duration-seconds 5}))]
        (is (= {:ok true
                :status :not-owner
                :backend :kubernetes-lease
                :election-id "orders"
                :owner-id "node-b"
                :fencing 8
                :remaining-ttl-ms 5000}
               (p/resign! component :orders "wrong-token" nil)))))

    (testing "status returns held and never leaks token"
      (with-redefs [kube/request! (fn [_ _]
                                    (lease-response {:name lease-name
                                                     :owner-id "node-a"
                                                     :token "token-1"
                                                     :lease-transitions 7
                                                     :renew-time-ms fixed-now-ms
                                                     :acquire-time-ms (- fixed-now-ms 500)
                                                     :lease-duration-seconds 4}))]
        (let [result (p/status component :orders nil)]
          (is (= {:ok true
                  :status :held
                  :backend :kubernetes-lease
                  :election-id "orders"
                  :owner-id "node-a"
                  :fencing 7
                  :remaining-ttl-ms 4000}
                 result))
          (is (false? (contains? result :token))))))

    (testing "status returns vacant for missing lease"
      (with-redefs [kube/request! (fn [_ _]
                                    (not-found-response lease-name))]
        (is (= {:ok true
                :status :vacant
                :backend :kubernetes-lease
                :election-id "orders"}
               (p/status component :orders nil)))))))

(deftest conflict-and-error-handling
  (let [component (make-component)
        lease-name (k8s/lease-name "dcore-leader-" "orders")]
    (testing "acquire returns busy after optimistic conflict"
      (let [responses (atom [(not-found-response lease-name)
                             {:status 409 :body {:kind "Status" :reason "Conflict"}}
                             (lease-response {:name lease-name
                                              :owner-id "node-b"
                                              :token "token-b"
                                              :lease-transitions 4
                                              :renew-time-ms fixed-now-ms
                                              :acquire-time-ms fixed-now-ms
                                              :lease-duration-seconds 6})])]
        (with-redefs [common/generate-token (fn [] "token-1")
                      kube/request! (fn [_ _]
                                      (let [response (first @responses)]
                                        (swap! responses rest)
                                        response))]
          (is (= {:ok true
                  :status :busy
                  :backend :kubernetes-lease
                  :election-id "orders"
                  :owner-id "node-b"
                  :fencing 4
                  :remaining-ttl-ms 6000}
                 (p/acquire! component :orders {:lease-ms 5000}))))))

    (testing "renew returns lost after conflict and reread"
      (let [responses (atom [(lease-response {:name lease-name
                                              :owner-id "node-a"
                                              :token "token-1"
                                              :resource-version "3"
                                              :lease-transitions 5
                                              :renew-time-ms fixed-now-ms
                                              :acquire-time-ms fixed-now-ms
                                              :lease-duration-seconds 6})
                             {:status 409 :body {:kind "Status" :reason "Conflict"}}
                             (lease-response {:name lease-name
                                              :owner-id "node-b"
                                              :token "token-b"
                                              :resource-version "4"
                                              :lease-transitions 6
                                              :renew-time-ms fixed-now-ms
                                              :acquire-time-ms fixed-now-ms
                                              :lease-duration-seconds 7})])]
        (with-redefs [kube/request! (fn [_ _]
                                      (let [response (first @responses)]
                                        (swap! responses rest)
                                        response))]
          (is (= {:ok true
                  :status :lost
                  :backend :kubernetes-lease
                  :election-id "orders"
                  :owner-id "node-b"
                  :fencing 6
                  :remaining-ttl-ms 7000}
                 (p/renew! component :orders "token-1" {:lease-ms 5000}))))))

    (testing "resign retries once after conflict if ownership is unchanged"
      (let [requests (atom [])
            responses (atom [(lease-response {:name lease-name
                                              :owner-id "node-a"
                                              :token "token-1"
                                              :resource-version "3"
                                              :lease-transitions 7
                                              :renew-time-ms fixed-now-ms
                                              :acquire-time-ms fixed-now-ms
                                              :lease-duration-seconds 6})
                             {:status 409 :body {:kind "Status" :reason "Conflict"}}
                             (lease-response {:name lease-name
                                              :owner-id "node-a"
                                              :token "token-1"
                                              :resource-version "4"
                                              :lease-transitions 7
                                              :renew-time-ms fixed-now-ms
                                              :acquire-time-ms fixed-now-ms
                                              :lease-duration-seconds 6})
                             (lease-response {:name lease-name
                                              :resource-version "5"
                                              :lease-transitions 7
                                              :lease-duration-seconds nil})])]
        (with-redefs [kube/request! (fn [_ request]
                                      (swap! requests conj request)
                                      (let [response (first @responses)]
                                        (swap! responses rest)
                                        response))]
          (is (= {:ok true
                  :status :released
                  :backend :kubernetes-lease
                  :election-id "orders"
                  :owner-id "node-a"
                  :fencing 7}
                 (p/resign! component :orders "token-1" nil)))
          (is (= 2 (count (filter #(= :put (:method %)) @requests))))))

    (testing "401/403 and malformed payloads raise explicit errors"
      (with-redefs [kube/request! (fn [_ _]
                                    {:status 403
                                     :body {:kind "Status" :reason "Forbidden"}})]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"not authorized"
                              (p/status component :orders nil))))
      (with-redefs [kube/request! (fn [_ _]
                                    {:status 200
                                     :body {:kind "Lease"
                                            :metadata {}
                                            :spec {:holderIdentity "node-a"
                                                   :renewTime "not-a-time"
                                                   :leaseDurationSeconds 5}}})]
        (is (thrown? clojure.lang.ExceptionInfo
                     (p/status component :orders nil))))))))

(deftest init-key-defaults-and-validation
  (testing "init-key applies defaults"
    (let [component (ig/init-key :d-core.core.leader-election.kubernetes-lease/kubernetes-lease
                                 {:kubernetes-client {:namespace "workers"}})]
      (is (= "workers" (:namespace component)))
      (is (= 15000 (:default-lease-ms component)))
      (is (= "dcore-leader-" (:lease-name-prefix component)))))

  (testing "init-key validates required kubernetes-client"
    (is (thrown? clojure.lang.ExceptionInfo
                 (ig/init-key :d-core.core.leader-election.kubernetes-lease/kubernetes-lease {})))))
