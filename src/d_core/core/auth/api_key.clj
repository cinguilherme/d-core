(ns d-core.core.auth.api-key
  (:require [clojure.string :as str]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [d-core.core.api-keys.protocol :as api-keys]))

(defn- request-ip
  [req]
  (let [xff (some-> (get-in req [:headers "x-forwarded-for"]) str/trim)
        first-ip (when (seq xff) (-> xff (str/split #",") first str/trim))]
    (or first-ip
        (get-in req [:headers "x-real-ip"])
        (:remote-addr req))))

(defn- method-allowed?
  [request-method allowed-methods]
  (if (seq allowed-methods)
    (let [normalize-method (fn [m]
                             (-> (if (keyword? m) (name m) (str m))
                                 str/lower-case
                                 keyword))]
      (contains? (set (map normalize-method allowed-methods))
                 (normalize-method request-method)))
    true))

(defn- path-match?
  [path pattern]
  (let [path (or path "")]
    (cond
    (nil? pattern) false
    (string? pattern)
    (if (str/ends-with? pattern "*")
      (str/starts-with? path (subs pattern 0 (dec (count pattern))))
      (= path pattern))
    (instance? java.util.regex.Pattern pattern)
    (boolean (re-matches pattern path))
    :else false)))

(defn- path-allowed?
  [uri allowlist]
  (if (seq allowlist)
    (boolean (some #(path-match? uri %) allowlist))
    true))

(defn- ip-allowed?
  [ip allowlist denylist]
  (cond
    (and (seq denylist) (contains? (set denylist) ip)) false
    (seq allowlist) (contains? (set allowlist) ip)
    :else true))

(defn- unauthorized-response
  [reason]
  {:status 403
   :body "Forbidden"
   :headers {"X-Auth-Reason" (name reason)}})

(defn- rate-limited-response
  [{:keys [retry-after-ms remaining reset-at]}]
  (let [retry-seconds (long (Math/ceil (/ (double (or retry-after-ms 0)) 1000.0)))]
    {:status 429
     :body "Rate limit exceeded"
     :headers {"Retry-After" (str (max 0 retry-seconds))
               "X-RateLimit-Remaining" (str (max 0 (long (or remaining 0))))
               "X-RateLimit-Reset" (str (long (or reset-at 0)))}}))

(defn- auth-api-key-principal
  [req]
  (let [principal (:auth/principal req)]
    (when (and principal
               (= :api-key (:auth-type principal))
               (seq (:api-key/id principal)))
      principal)))

(defn- evaluate-request-constraints
  [req limits]
  (let [method-allowlist (:method-allowlist limits)
        path-allowlist (:path-allowlist limits)
        ip-allowlist (:ip-allowlist limits)
        ip-denylist (:ip-denylist limits)
        request-method (:request-method req)
        uri (:uri req)
        ip (request-ip req)]
    (cond
      (not (method-allowed? request-method method-allowlist))
      {:allowed? false :reason :method-not-allowed}

      (not (path-allowed? uri path-allowlist))
      {:allowed? false :reason :path-not-allowed}

      (not (ip-allowed? ip ip-allowlist ip-denylist))
      {:allowed? false :reason :ip-not-allowed}

      :else
      {:allowed? true})))

(defn- consume-rate-limit
  [api-key-store api-key-id limits]
  (let [{:keys [limit window-ms]} (:rate-limit limits)]
    (if (and (number? limit) (number? window-ms) (pos? limit) (pos? window-ms))
      (api-keys/consume-rate-limit! api-key-store api-key-id {:limit (long limit)
                                                              :window-ms (long window-ms)})
      {:allowed? true :remaining Long/MAX_VALUE :reset-at nil :retry-after-ms nil})))

(defn wrap-api-key-limitations
  [handler api-key-store {:keys [on-deny on-rate-limit logger] :as _opts}]
  (fn [req]
    (if-let [principal (auth-api-key-principal req)]
      (let [api-key-id (:api-key/id principal)
            limits (or (:api-key/limits principal) {})
            request-check (evaluate-request-constraints req limits)]
        (if-not (:allowed? request-check)
          (do
            (when logger
              (logger/log logger :warn ::api-key-request-denied
                          {:api-key-id api-key-id
                           :reason (:reason request-check)}))
            (if on-deny
              (on-deny req request-check)
              (unauthorized-response (:reason request-check))))
          (let [rate-check (consume-rate-limit api-key-store api-key-id limits)]
            (if (:allowed? rate-check)
              (handler req)
              (do
                (when logger
                  (logger/log logger :warn ::api-key-rate-limited
                              {:api-key-id api-key-id
                               :remaining (:remaining rate-check)
                               :retry-after-ms (:retry-after-ms rate-check)}))
                (if on-rate-limit
                  (on-rate-limit req rate-check)
                  (rate-limited-response rate-check)))))))
      (handler req))))

(defmethod ig/init-key :d-core.core.auth.api-key/limitations-middleware
  [_ {:keys [api-key-store opts logger]}]
  (when-not api-key-store
    (throw (ex-info "API key limitations middleware requires :api-key-store"
                    {:type ::missing-api-key-store})))
  (when logger
    (logger/log logger :info ::api-key-limitations-middleware-initialized))
  (fn [handler]
    (wrap-api-key-limitations handler api-key-store (merge {:logger logger} (or opts {})))))
