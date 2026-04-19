(ns d-core.core.leader-election.backend-contract-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.leader-election.common :as common]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.core.leader-election.observability :as obs]
            [d-core.core.leader-election.protocol :as p]
            [d-core.core.leader-election.redis-common :as redis-common]
            [d-core.core.leader-election.redis :as redis]
            [d-core.core.leader-election.valkey :as valkey]
            [d-core.helpers.logger :as h-logger]
            [d-core.helpers.metrics :as h-metrics]
            [duct.logger :as logger]
            [integrant.core :as ig]))

(def ^:private fixed-now-ms
  1700000000000)

(def ^:private backend-cases
  [{:name :redis
    :backend :redis
    :make-component (fn [observability]
                      (redis/->RedisLeaderElection :client
                                                   "node-1"
                                                   "dcore:leader-election:"
                                                   15000
                                                   (constantly fixed-now-ms)
                                                   observability))
    :acquire-var #'d-core.core.leader-election.redis/eval-acquire!
    :renew-var #'d-core.core.leader-election.redis/eval-renew!
    :resign-var #'d-core.core.leader-election.redis/eval-resign!
    :status-var #'d-core.core.leader-election.redis/eval-status
    :init-key :d-core.core.leader-election.redis/redis
    :client-key :redis-client}
   {:name :valkey
    :backend :valkey
    :make-component (fn [observability]
                      (valkey/->ValkeyLeaderElection :client
                                                     "node-1"
                                                     "dcore:leader-election:"
                                                     15000
                                                     (constantly fixed-now-ms)
                                                     observability))
    :acquire-var #'d-core.core.leader-election.valkey/eval-acquire!
    :renew-var #'d-core.core.leader-election.valkey/eval-renew!
    :resign-var #'d-core.core.leader-election.valkey/eval-resign!
    :status-var #'d-core.core.leader-election.valkey/eval-status
    :init-key :d-core.core.leader-election.valkey/valkey
    :client-key :valkey-client}])

(defn- make-observed-component
  [make-component]
  (let [{:keys [logger logs]} (h-logger/make-test-logger)
        {:keys [metrics calls]} (h-metrics/make-test-metrics)]
    {:component (make-component (obs/make-context logger metrics))
     :logs logs
     :metric-calls calls
     :logger logger
     :metrics metrics}))

(defn- request-metric-labels
  [metric-calls]
  (get-in (first (h-metrics/find-calls metric-calls :inc! :leader_election_requests_total))
          [:metric :labels]))

(defn- error-metric-labels
  [metric-calls]
  (get-in (first (h-metrics/find-calls metric-calls :inc! :leader_election_errors_total))
          [:metric :labels]))

(defn- duration-metric-labels
  [metric-calls]
  (get-in (first (h-metrics/find-calls metric-calls :observe! :leader_election_request_duration_seconds))
          [:metric :labels]))

(definterface ILabelable
  (labels [^"[Ljava.lang.String;" label-values]))

(defn- make-labelable
  [collector-name]
  (reify ILabelable
    (labels [_ label-values]
      {:collector collector-name
       :labels (vec label-values)})))

(defn- make-throwing-metrics
  [error]
  (reify metrics/MetricsProtocol
    (registry [_] nil)
    (counter [_ opts]
      (make-labelable (:name opts)))
    (gauge [_ opts]
      (make-labelable (:name opts)))
    (histogram [_ opts]
      (make-labelable (:name opts)))
    (inc! [_ _]
      (throw error))
    (inc! [_ _ _]
      (throw error))
    (observe! [_ _ _]
      (throw error))))

(defn- make-throwing-logger
  [error]
  (reify logger/Logger
    (-log [_ _ _ _ _ _ _ _]
      (throw error))))

(deftest acquire-contracts
  (doseq [{:keys [name backend make-component acquire-var]} backend-cases]
    (testing (str name " acquire returns acquired result shape")
      (let [component (make-component nil)
            calls (atom [])]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [client lease-key fencing-key owner-id token now-ms lease-ms]
                                       (swap! calls conj {:client client
                                                          :lease-key lease-key
                                                          :fencing-key fencing-key
                                                          :owner-id owner-id
                                                          :token token
                                                          :now-ms now-ms
                                                          :lease-ms lease-ms})
                                       ["acquired" owner-id "7" token lease-ms])}
          (fn []
            (is (= {:ok true
                    :status :acquired
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7
                    :remaining-ttl-ms 15000
                    :token "token-1"}
                   (p/acquire! component :orders nil)))
            (is (= [{:client :client
                     :lease-key "dcore:leader-election:orders:lease"
                     :fencing-key "dcore:leader-election:orders:fencing"
                     :owner-id "node-1"
                     :token "token-1"
                     :now-ms fixed-now-ms
                     :lease-ms 15000}]
                   @calls))))))

    (testing (str name " acquire returns busy without leaking token")
      (let [component (make-component nil)]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [& _]
                                       ["busy" "node-2" "9" "" "1200"])}
          (fn []
            (let [result (p/acquire! component "orders" {:lease-ms 2500})]
              (is (= {:ok true
                      :status :busy
                      :backend backend
                      :election-id "orders"
                      :owner-id "node-2"
                      :fencing 9
                      :remaining-ttl-ms 1200}
                     result))
              (is (false? (contains? result :token))))))))))

(deftest renew-contracts
  (doseq [{:keys [name backend make-component renew-var]} backend-cases]
    (testing (str name " renew returns renewed result shape")
      (let [component (make-component nil)
            calls (atom [])]
        (with-redefs-fn {renew-var (fn [client lease-key token now-ms lease-ms]
                                     (swap! calls conj {:client client
                                                        :lease-key lease-key
                                                        :token token
                                                        :now-ms now-ms
                                                        :lease-ms lease-ms})
                                     ["renewed" "node-1" "7" token lease-ms])}
          (fn []
            (is (= {:ok true
                    :status :renewed
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7
                    :remaining-ttl-ms 9000
                    :token "token-1"}
                   (p/renew! component :orders "token-1" {:lease-ms 9000})))
            (is (= [{:client :client
                     :lease-key "dcore:leader-election:orders:lease"
                     :token "token-1"
                     :now-ms fixed-now-ms
                     :lease-ms 9000}]
                   @calls))))))

    (testing (str name " renew returns lost when token does not match")
      (let [component (make-component nil)]
        (with-redefs-fn {renew-var (fn [& _]
                                     ["lost" "node-2" "11" "2500"])}
          (fn []
            (let [result (p/renew! component "orders" "stale-token" nil)]
              (is (= {:ok true
                      :status :lost
                      :backend backend
                      :election-id "orders"
                      :owner-id "node-2"
                      :fencing 11
                      :remaining-ttl-ms 2500}
                     result))
              (is (false? (contains? result :token))))))))))

(deftest resign-and-status-contracts
  (doseq [{:keys [name backend make-component resign-var status-var]} backend-cases]
    (testing (str name " resign returns released and not-owner statuses")
      (let [component (make-component nil)]
        (with-redefs-fn {resign-var (fn [_client _lease-key token]
                                      (if (= token "token-1")
                                        ["released" "node-1" "7"]
                                        ["not-owner" "node-2" "8" "2000"]))}
          (fn []
            (is (= {:ok true
                    :status :released
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7}
                   (p/resign! component :orders "token-1" nil)))
            (is (= {:ok true
                    :status :not-owner
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-2"
                    :fencing 8
                    :remaining-ttl-ms 2000}
                   (p/resign! component :orders "wrong-token" nil)))))))

    (testing (str name " status returns held without token")
      (let [component (make-component nil)]
        (with-redefs-fn {status-var (fn [_client _lease-key]
                                      ["held" "node-1" "7" "3000"])}
          (fn []
            (let [result (p/status component :orders nil)]
              (is (= {:ok true
                      :status :held
                      :backend backend
                      :election-id "orders"
                      :owner-id "node-1"
                      :fencing 7
                      :remaining-ttl-ms 3000}
                     result))
              (is (false? (contains? result :token))))))))

    (testing (str name " status returns vacant")
      (let [component (make-component nil)]
        (with-redefs-fn {status-var (fn [_client _lease-key] ["vacant"])}
          (fn []
            (is (= {:ok true
                    :status :vacant
                    :backend backend
                    :election-id "orders"}
                   (p/status component :orders nil)))))))))

(deftest observability-contracts
  (doseq [{:keys [name backend make-component acquire-var renew-var resign-var status-var]} backend-cases]
    (testing (str name " acquire success logs and records metrics")
      (let [{:keys [component logs metric-calls]} (make-observed-component make-component)]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [& _]
                                       ["acquired" "node-1" "7" "token-1" "15000"])}
          (fn []
            (is (= :acquired (:status (p/acquire! component :orders nil))))
            (is (= [(clojure.core/name backend) "acquire" "acquired"]
                   (request-metric-labels metric-calls)))
            (is (= [(clojure.core/name backend) "acquire"]
                   (duration-metric-labels metric-calls)))
            (is (= [{:level :info
                     :event ::obs/operation-result
                     :data {:backend backend
                            :op :acquire
                            :status :acquired
                            :election-id "orders"
                            :owner-id "node-1"
                            :fencing 7
                            :remaining-ttl-ms 15000}}]
                   @logs))))))

    (testing (str name " busy, renewed, lost, released, not-owner, and status outcomes are observable")
      (let [{:keys [component logs metric-calls]} (make-observed-component make-component)]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [& _] ["busy" "node-2" "9" "" "1200"])
                         renew-var (fn [_ _ token _ _]
                                     (if (= token "token-1")
                                       ["renewed" "node-1" "7" token "9000"]
                                       ["lost" "node-2" "11" "2500"]))
                         resign-var (fn [_ _ token]
                                      (if (= token "token-1")
                                        ["released" "node-1" "7"]
                                        ["not-owner" "node-2" "8" "2000"]))
                         status-var (fn [_ _]
                                      ["held" "node-1" "7" "3000"])}
          (fn []
            (is (= :busy (:status (p/acquire! component :orders nil))))
            (is (= :renewed (:status (p/renew! component :orders "token-1" {:lease-ms 9000}))))
            (is (= :lost (:status (p/renew! component :orders "wrong-token" nil))))
            (is (= :released (:status (p/resign! component :orders "token-1" nil))))
            (is (= :not-owner (:status (p/resign! component :orders "wrong-token" nil))))
            (is (= :held (:status (p/status component :orders nil))))
            (is (= [["debug" "busy"]
                    ["debug" "renewed"]
                    ["warn" "lost"]
                    ["info" "released"]
                    ["debug" "not-owner"]]
                   (mapv (fn [{:keys [level data]}]
                           [(clojure.core/name level) (clojure.core/name (:status data))])
                         @logs)))
            (is (= #{"busy" "renewed" "lost" "released" "not-owner" "held"}
                   (set (map last
                             (map :labels
                                  (map :metric
                                       (h-metrics/find-calls metric-calls :inc! :leader_election_requests_total)))))))
            (is (= 6 (count (h-metrics/find-calls metric-calls :observe! :leader_election_request_duration_seconds))))
            (is (not-any? #(= ::obs/operation-result (:event %))
                          (filter (fn [{:keys [data]}]
                                    (contains? #{:held :vacant} (:status data)))
                                  @logs)))))))

    (testing (str name " backend exceptions emit one error metric and log then rethrow")
      (let [{:keys [component logs metric-calls]} (make-observed-component make-component)
            boom (RuntimeException. "boom")]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [& _] (throw boom))}
          (fn []
            (is (thrown-with-msg? RuntimeException #"boom"
                                  (p/acquire! component :orders nil)))
            (is (= [(clojure.core/name backend) "acquire" "exception"]
                   (error-metric-labels metric-calls)))
            (is (= [(clojure.core/name backend) "acquire"]
                   (duration-metric-labels metric-calls)))
            (is (= [{:level :error
                     :event ::obs/operation-failed
                     :data {:backend backend
                            :op :acquire
                            :type :exception
                            :error "boom"
                            :election-id "orders"}}]
                   @logs))))))

  (doseq [{:keys [name backend make-component acquire-var status-var]} backend-cases]
    (testing (str name " metrics failures do not change a successful acquire")
      (let [metrics-boom (RuntimeException. "metrics boom")
            component (make-component (obs/make-context nil (make-throwing-metrics metrics-boom)))]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [& _]
                                       ["acquired" "node-1" "7" "token-1" "15000"])}
          (fn []
            (is (= {:ok true
                    :status :acquired
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7
                    :remaining-ttl-ms 15000
                    :token "token-1"}
                   (p/acquire! component :orders nil)))))))

    (testing (str name " status returns normally even with a throwing logger")
      (let [logger-boom (RuntimeException. "logger boom")
            component (make-component (obs/make-context (make-throwing-logger logger-boom) nil))]
        (with-redefs-fn {status-var (fn [_ _]
                                      ["held" "node-1" "7" "3000"])}
          (fn []
            (is (= {:ok true
                    :status :held
                    :backend backend
                    :election-id "orders"
                    :owner-id "node-1"
                    :fencing 7
                    :remaining-ttl-ms 3000}
                   (p/status component :orders nil)))))))

    (testing (str name " broken observability does not mask backend exceptions")
      (let [backend-boom (RuntimeException. "backend boom")
            metrics-boom (RuntimeException. "metrics boom")
            logger-boom (RuntimeException. "logger boom")
            component (make-component (obs/make-context (make-throwing-logger logger-boom)
                                                        (make-throwing-metrics metrics-boom)))]
        (with-redefs-fn {#'d-core.core.leader-election.common/generate-token (fn [] "token-1")
                         acquire-var (fn [& _]
                                       (throw backend-boom))}
          (fn []
            (try
              (p/acquire! component :orders nil)
              (is false "Expected backend exception")
              (catch RuntimeException ex
                (is (identical? backend-boom ex))
                (is (= "backend boom" (.getMessage ex)))))))))))
  )

(deftest init-key-defaults-and-validation
  (doseq [{:keys [name init-key client-key]} backend-cases]
    (testing (str name " init-key applies defaults")
      (let [component (ig/init-key init-key {client-key :client})]
        (is (= redis-common/default-prefix (:prefix component)))
        (is (= common/default-lease-ms (:default-lease-ms component)))
        (is (string? (:owner-id component)))
        (is (re-find #":" (:owner-id component)))))

    (testing (str name " init-key accepts logger and metrics")
      (let [{:keys [logger]} (h-logger/make-test-logger)
            {:keys [metrics]} (h-metrics/make-test-metrics)
            component (ig/init-key init-key {client-key :client
                                             :logger logger
                                             :metrics metrics})]
        (is (= logger (get-in component [:observability :logger])))
        (is (= metrics (get-in component [:observability :metrics])))))

    (testing (str name " init-key validates required client")
      (is (thrown? clojure.lang.ExceptionInfo
                   (ig/init-key init-key {}))))

    (testing (str name " init-key validates metrics dependency")
      (let [ex (try
                 (ig/init-key init-key {client-key :client
                                        :metrics {}})
                 nil
                 (catch clojure.lang.ExceptionInfo ex
                   ex))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (= "d-core.core.metrics.protocol/MetricsProtocol"
               (:expected (ex-data ex))))))

    (testing (str name " init-key validates default-lease-ms with typed errors")
      (doseq [invalid [nil "10" :x 1.5]]
        (let [ex (try
                   (ig/init-key init-key {client-key :client
                                          :default-lease-ms invalid})
                   nil
                   (catch clojure.lang.ExceptionInfo ex
                     ex))]
          (is (instance? clojure.lang.ExceptionInfo ex))
          (is (= ::common/invalid-field (:type (ex-data ex))))
          (is (= :default-lease-ms (:field (ex-data ex))))
          (is (= invalid (:value (ex-data ex)))))))))
