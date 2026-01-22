(ns d-core.core.http.client
  (:require
   [clj-http.client :as http])
  (:import
   (java.util.concurrent Semaphore TimeUnit)))

(defrecord HttpClient [id base-url default-headers http-opts policies])

(defn- now-ms []
  (System/currentTimeMillis))

(defn- policy-enabled?
  [policy]
  (and policy (get-in policy [:config :enabled] true)))

(defn- init-rate-limit
  [{:keys [rate-per-sec burst] :as config}]
  (when config
    (let [rate-per-sec (double (or rate-per-sec 100))
          burst (double (or burst rate-per-sec))]
      {:config (merge {:enabled true
                       :rate-per-sec rate-per-sec
                       :burst burst
                       :timeout-ms 0}
                      config)
       :state (atom {:tokens burst
                     :last-refill-ms (now-ms)})})))

(defn- init-bulkhead
  [{:keys [max-concurrent] :as config}]
  (when config
    (let [max-concurrent (long (or max-concurrent 50))]
      (when (<= max-concurrent 0)
        (throw (ex-info "Bulkhead max-concurrent must be > 0"
                        {:type ::invalid-bulkhead
                         :max-concurrent max-concurrent})))
      {:config (merge {:enabled true
                       :max-concurrent max-concurrent
                       :timeout-ms 0}
                      config)
       :semaphore (Semaphore. max-concurrent)})))

(defn- init-circuit-breaker
  [config]
  (when config
    {:config (merge {:enabled true
                     :failure-threshold 5
                     :window-ms 10000
                     :reset-timeout-ms 30000
                     :half-open-max 1
                     :failure-statuses #{500 502 503 504}}
                    config)
     :state (atom {:state :closed
                   :failures []
                   :opened-at nil
                   :half-open-inflight 0})}))

(defn- init-retry
  [config]
  (when config
    {:config (merge {:enabled true
                     :max-attempts 3
                     :allow-non-idempotent? false
                     :retry-statuses #{429 500 502 503 504}
                     :retry-exceptions [java.io.IOException
                                        java.net.SocketTimeoutException]
                     :backoff {:base-ms 100
                               :max-ms 2000
                               :multiplier 2.0
                               :jitter 0.2}}
                    config)}))

(defn- build-policies
  [policies]
  (let [{:keys [rate-limit bulkhead circuit-breaker retry]} policies]
    {:rate-limit (init-rate-limit rate-limit)
     :bulkhead (init-bulkhead bulkhead)
     :circuit-breaker (init-circuit-breaker circuit-breaker)
     :retry (init-retry retry)}))

(defn make-client
  [{:keys [id base-url default-headers http-opts policies]}]
  (->HttpClient
    id
    base-url
    default-headers
    (merge {:throw-exceptions false} http-opts)
    (build-policies policies)))

(defn- merge-policy-config
  [policy override]
  (if (and policy override)
    (update policy :config merge override)
    policy))

(defn- merge-policies
  [client req]
  (let [override (:policies req)]
    (-> (:policies client)
        (update :rate-limit merge-policy-config (:rate-limit override))
        (update :bulkhead merge-policy-config (:bulkhead override))
        (update :circuit-breaker merge-policy-config (:circuit-breaker override))
        (update :retry merge-policy-config (:retry override)))))

(defn- rate-limit-acquire!
  [{:keys [config state]}]
  (let [{:keys [rate-per-sec burst timeout-ms]} config
        rate-per-sec (double rate-per-sec)
        _ (when (<= rate-per-sec 0.0)
            (throw (ex-info "Rate limit rate-per-sec must be > 0"
                            {:type ::invalid-rate-limit
                             :rate-per-sec rate-per-sec})))
        rate-per-ms (/ rate-per-sec 1000.0)
        timeout-ms (long (or timeout-ms 0))
        deadline (+ (now-ms) timeout-ms)]
    (loop []
      (let [now (now-ms)
            [ok wait-ms] (locking state
                           (let [{:keys [tokens last-refill-ms]} @state
                                 last-refill-ms (or last-refill-ms now)
                                 elapsed (- now last-refill-ms)
                                 refill (+ (double tokens) (* elapsed rate-per-ms))
                                 tokens (min (double burst) refill)
                                 ok (>= tokens 1.0)
                                 new-tokens (if ok (- tokens 1.0) tokens)
                                 missing (max 0.0 (- 1.0 tokens))
                                 wait-ms (long (Math/ceil (/ missing rate-per-ms)))]
                             (reset! state {:tokens new-tokens
                                            :last-refill-ms now})
                             [ok wait-ms]))]
        (cond
          ok true
          (<= timeout-ms 0) (throw (ex-info "Rate limit exceeded" {:type ::rate-limited}))
          (> (now-ms) deadline) (throw (ex-info "Rate limit timeout" {:type ::rate-limited}))
          :else (do (Thread/sleep (max 1 (min 50 wait-ms)))
                    (recur)))))))

(defn- bulkhead-acquire!
  [{:keys [config semaphore]}]
  (let [{:keys [timeout-ms]} config
        timeout-ms (long (or timeout-ms 0))]
    (if (<= timeout-ms 0)
      (when-not (.tryAcquire semaphore)
        (throw (ex-info "Bulkhead full" {:type ::bulkhead-full})))
      (when-not (.tryAcquire semaphore timeout-ms TimeUnit/MILLISECONDS)
        (throw (ex-info "Bulkhead timeout" {:type ::bulkhead-timeout}))))))

(defn- bulkhead-release!
  [{:keys [semaphore]}]
  (.release semaphore))

(defn- response-failure?
  [resp {:keys [failure-statuses failure-pred]}]
  (cond
    failure-pred (boolean (failure-pred resp))
    (and failure-statuses (:status resp)) (contains? failure-statuses (:status resp))
    :else false))

(defn- circuit-before!
  [{:keys [config state]}]
  (let [{:keys [window-ms reset-timeout-ms half-open-max]} config]
    (locking state
      (let [{:keys [state failures opened-at half-open-inflight]} @state
            now (now-ms)
            failures (vec (filter #(> (+ % window-ms) now) failures))
            opened-at (or opened-at now)]
        (cond
          (= state :open)
          (if (>= (- now opened-at) reset-timeout-ms)
            (if (< half-open-inflight half-open-max)
              (do (reset! state {:state :half-open
                                 :failures failures
                                 :opened-at opened-at
                                 :half-open-inflight (inc half-open-inflight)})
                  {:allowed? true :half-open? true})
              {:allowed? false})
            {:allowed? false})

          (= state :half-open)
          (if (< half-open-inflight half-open-max)
            (do (reset! state {:state :half-open
                               :failures failures
                               :opened-at opened-at
                               :half-open-inflight (inc half-open-inflight)})
                {:allowed? true :half-open? true})
            {:allowed? false})

          :else
          (do (reset! state {:state :closed
                             :failures failures
                             :opened-at opened-at
                             :half-open-inflight half-open-inflight})
              {:allowed? true :half-open? false}))))))

(defn- circuit-after!
  [{:keys [config state]} {:keys [half-open?]} failed?]
  (let [{:keys [failure-threshold window-ms]} config
        now (now-ms)]
    (locking state
      (let [{:keys [state failures opened-at half-open-inflight]} @state
            failures (vec (filter #(> (+ % window-ms) now) failures))
            failures (if failed? (conj failures now) failures)
            half-open-inflight (if half-open? (max 0 (dec half-open-inflight)) half-open-inflight)
            opened-at (or opened-at now)]
        (cond
          (and half-open? failed?)
          (reset! state {:state :open
                         :failures failures
                         :opened-at now
                         :half-open-inflight half-open-inflight})

          (and half-open? (not failed?))
          (reset! state {:state :closed
                         :failures []
                         :opened-at nil
                         :half-open-inflight 0})

          (and (= state :closed) failed? (>= (count failures) failure-threshold))
          (reset! state {:state :open
                         :failures failures
                         :opened-at now
                         :half-open-inflight half-open-inflight})

          :else
          (reset! state {:state (if (= state :open) :open :closed)
                         :failures failures
                         :opened-at opened-at
                         :half-open-inflight half-open-inflight}))))))

(defn- retry-eligible-method?
  [method {:keys [allow-non-idempotent?]}]
  (or allow-non-idempotent?
      (contains? #{:get :head :put :delete :options} method)))

(defn- retry-eligible-exception?
  [e {:keys [retry-exceptions]}]
  (boolean (some #(instance? % e) retry-exceptions)))

(defn- response-retry?
  [resp {:keys [retry-statuses retry-pred]}]
  (cond
    retry-pred (boolean (retry-pred resp))
    (and retry-statuses (:status resp)) (contains? retry-statuses (:status resp))
    :else false))

(defn- calc-backoff-ms
  [attempt {:keys [base-ms max-ms multiplier jitter]}]
  (let [base (double (or base-ms 50))
        multiplier (double (or multiplier 2.0))
        max-ms (double (or max-ms 1000))
        raw (* base (Math/pow multiplier (dec attempt)))
        capped (min raw max-ms)
        jitter (double (or jitter 0.2))
        jitter-range (* capped jitter)
        sleep-ms (+ capped (- (rand (* 2 jitter-range)) jitter-range))]
    (long (max 0.0 sleep-ms))))

(defn- resolve-url
  [base-url {:keys [url path]}]
  (cond
    url url
    (and base-url path) (str (str base-url) (if (.startsWith (str path) "/") "" "/") path)
    base-url (str base-url)
    :else nil))

(defn request!
  "Executes an HTTP request with optional rate-limit, circuit-breaker,
  bulkhead, and retry policies.

  Request accepts clj-http options plus:
  - :path (resolved against client :base-url)
  - :policies {:rate-limit {...} :bulkhead {...} :circuit-breaker {...} :retry {...}}"
  [^HttpClient client {:keys [method headers] :as req}]
  (let [policies (merge-policies client req)
        rate-limit (:rate-limit policies)
        bulkhead (:bulkhead policies)
        circuit-breaker (:circuit-breaker policies)
        retry (:retry policies)
        request-opts (-> (:http-opts client)
                         (merge (dissoc req :policies :path))
                         (assoc :method method
                                :url (resolve-url (:base-url client) req))
                         (update :headers merge (:default-headers client) headers))]
    (when-not (:url request-opts)
      (throw (ex-info "Missing :url or :path for HTTP request"
                      {:type ::missing-url
                       :client (:id client)})))
    (when (and method (not (keyword? method)))
      (throw (ex-info "Request :method must be a keyword" {:type ::invalid-method
                                                           :method method})))
    (let [retry-config (when (policy-enabled? retry) (:config retry))
          max-attempts (long (or (:max-attempts retry-config) 1))
          retry-backoff (:backoff retry-config)]
      (loop [attempt 1]
        (when (policy-enabled? bulkhead)
          (bulkhead-acquire! bulkhead))
        (try
          (let [gate (when (policy-enabled? circuit-breaker)
                       (circuit-before! circuit-breaker))]
            (when (and gate (not (:allowed? gate)))
              (throw (ex-info "Circuit breaker open"
                              {:type ::circuit-open
                               :client (:id client)})))
            (let [result (try
                           (when (policy-enabled? rate-limit)
                             (rate-limit-acquire! rate-limit))
                           (http/request request-opts)
                           (catch Exception e
                             e))
                  failed? (or (instance? Exception result)
                              (and (policy-enabled? circuit-breaker)
                                   (response-failure? result (:config circuit-breaker))))
                  eligible-method? (when retry-config
                                     (retry-eligible-method? method retry-config))
                  retryable? (and retry-config eligible-method?
                                  (or (and (instance? Exception result)
                                           (retry-eligible-exception? result retry-config))
                                      (and (map? result)
                                           (response-retry? result retry-config))))
                  can-retry? (and retryable? (< attempt max-attempts))]
              (when (policy-enabled? circuit-breaker)
                (circuit-after! circuit-breaker gate failed?))
              (cond
                (and (instance? Exception result) can-retry?)
                (do (Thread/sleep (calc-backoff-ms attempt retry-backoff))
                    (recur (inc attempt)))

                (and (map? result) can-retry?)
                (do (Thread/sleep (calc-backoff-ms attempt retry-backoff))
                    (recur (inc attempt)))

                (instance? Exception result)
                (throw result)

                :else result)))
          (finally
            (when (policy-enabled? bulkhead)
              (bulkhead-release! bulkhead))))))))
