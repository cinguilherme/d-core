(ns d-core.core.leader-election.kubernetes-lease
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.core.clients.kubernetes.client :as kube]
            [d-core.core.leader-election.common :as common]
            [d-core.core.leader-election.protocol :as p]
            [integrant.core :as ig])
  (:import (java.security MessageDigest)
           (java.time Instant)))

(def default-lease-name-prefix
  "dcore-leader-")

(def ^:private token-annotation
  "dcore.io/leader-election-token")

(def ^:private election-id-annotation
  "dcore.io/leader-election-id")

(def ^:private max-lease-name-length
  63)

(def ^:private lease-name-hash-length
  12)

(def ^:private max-normalized-prefix-length
  (- max-lease-name-length lease-name-hash-length))

(defn normalize-lease-name-prefix
  [value]
  (let [raw (or value default-lease-name-prefix)
        sanitized (-> raw
                      str
                      str/lower-case
                      (str/replace #"[^a-z0-9-]+" "-")
                      (str/replace #"-+" "-")
                      (str/replace #"^-+" "")
                      (str/replace #"-+$" ""))]
    (when (str/blank? sanitized)
      (throw (ex-info "Kubernetes lease name prefix must not be blank after sanitization"
                      {:type ::invalid-lease-name-prefix
                       :lease-name-prefix value})))
    (let [normalized (str sanitized "-")]
      (when (> (count normalized) max-normalized-prefix-length)
        (throw (ex-info "Kubernetes lease name prefix is too long after sanitization"
                        {:type ::invalid-lease-name-prefix
                         :lease-name-prefix value
                         :normalized-prefix normalized
                         :max-length max-normalized-prefix-length})))
      normalized)))

(defn- sha256-hex
  [value]
  (let [digest (MessageDigest/getInstance "SHA-256")
        bytes (.digest digest (.getBytes (str value) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and % 0xff)) bytes))))

(defn lease-name
  [prefix election-id]
  (let [prefix (normalize-lease-name-prefix prefix)
        safe-id (-> election-id
                    str
                    str/lower-case
                    (str/replace #"[^a-z0-9-]+" "-")
                    (str/replace #"-+" "-")
                    (str/replace #"^-+" "")
                    (str/replace #"-+$" ""))
        hash-part (subs (sha256-hex election-id) 0 lease-name-hash-length)
        max-safe-id-len (max 0 (- max-lease-name-length (count prefix) (count hash-part) 1))
        id-part (subs (or safe-id "") 0 (min (count safe-id) max-safe-id-len))]
    (if (seq id-part)
      (str prefix id-part "-" hash-part)
      (str (subs prefix 0 (dec (count prefix))) "-" hash-part))))

(defn lease-duration-seconds
  [lease-ms]
  (max 1 (long (Math/ceil (/ (double lease-ms) 1000.0)))))

(defn- timestamp
  [epoch-ms]
  (.toString (Instant/ofEpochMilli (long epoch-ms))))

(defn- parse-rfc3339-ms
  [value field]
  (when (some? value)
    (when-not (string? value)
      (throw (ex-info "Kubernetes Lease field must be an RFC3339 string"
                      {:type ::invalid-lease-payload
                       :field field
                       :value value})))
    (try
      (.toEpochMilli (Instant/parse value))
      (catch Exception ex
        (throw (ex-info "Failed to parse Kubernetes Lease timestamp"
                        {:type ::invalid-lease-payload
                         :field field
                         :value value}
                        ex))))))

(defn- parse-positive-int
  [value field]
  (when (some? value)
    (let [parsed (cond
                   (integer? value) (long value)
                   (number? value) (long value)
                   (string? value) (common/parse-long-safe value)
                   :else nil)]
      (when-not parsed
        (throw (ex-info "Kubernetes Lease field must be numeric"
                        {:type ::invalid-lease-payload
                         :field field
                         :value value})))
      (when (<= parsed 0)
        (throw (ex-info "Kubernetes Lease field must be greater than zero"
                        {:type ::invalid-lease-payload
                         :field field
                         :value value})))
      parsed)))

(defn- parse-non-negative-int
  [value field]
  (when (some? value)
    (let [parsed (cond
                   (integer? value) (long value)
                   (number? value) (long value)
                   (string? value) (common/parse-long-safe value)
                   :else nil)]
      (when-not parsed
        (throw (ex-info "Kubernetes Lease field must be numeric"
                        {:type ::invalid-lease-payload
                         :field field
                         :value value})))
      (when (< parsed 0)
        (throw (ex-info "Kubernetes Lease field must not be negative"
                        {:type ::invalid-lease-payload
                         :field field
                         :value value})))
      parsed)))

(defn- parse-string-field
  [value field]
  (when (some? value)
    (when-not (string? value)
      (throw (ex-info "Kubernetes Lease field must be a string"
                      {:type ::invalid-lease-payload
                       :field field
                       :value value})))
    value))

(defn- metadata-map
  [body]
  (let [metadata (:metadata body)]
    (cond
      (nil? metadata) {}
      (map? metadata) metadata
      :else
      (throw (ex-info "Kubernetes Lease payload metadata must be a map"
                      {:type ::invalid-lease-payload
                       :field :metadata
                       :value metadata})))))

(defn- spec-map
  [body]
  (let [spec (:spec body)]
    (cond
      (nil? spec) {}
      (map? spec) spec
      :else
      (throw (ex-info "Kubernetes Lease payload spec must be a map"
                      {:type ::invalid-lease-payload
                       :field :spec
                       :value spec})))))

(defn- annotations-map
  [metadata]
  (let [annotations (:annotations metadata)]
    (cond
      (nil? annotations) {}
      (map? annotations) annotations
      :else
      (throw (ex-info "Kubernetes Lease annotations must be a map"
                      {:type ::invalid-lease-payload
                       :field :metadata.annotations
                       :value annotations})))))

(defn- labels-map
  [metadata]
  (let [labels (:labels metadata)]
    (cond
      (nil? labels) nil
      (map? labels) labels
      :else
      (throw (ex-info "Kubernetes Lease labels must be a map"
                      {:type ::invalid-lease-payload
                       :field :metadata.labels
                       :value labels})))))

(defn- lease-body
  [response]
  (let [body (:body response)]
    (when-not (map? body)
      (throw (ex-info "Kubernetes Lease response body must be a map"
                      {:type ::provider-error
                       :backend :kubernetes-lease
                       :status (:status response)
                       :response response
                       :body body})))
    (when-not (= "Lease" (:kind body))
      (throw (ex-info "Kubernetes API response was not a Lease"
                      {:type ::provider-error
                       :backend :kubernetes-lease
                       :status (:status response)
                       :response response
                       :body body})))
    body))

(defn- parse-lease
  [response]
  (let [body (lease-body response)
        metadata (metadata-map body)
        spec (spec-map body)
        annotations (annotations-map metadata)]
    {:resource-version (parse-string-field (:resourceVersion metadata) :metadata.resourceVersion)
     :labels (labels-map metadata)
     :annotations annotations
     :raw-spec spec
     :holder-identity (parse-string-field (:holderIdentity spec) :spec.holderIdentity)
     :token (parse-string-field (get annotations token-annotation) :metadata.annotations.token)
     :election-id (parse-string-field (get annotations election-id-annotation) :metadata.annotations.election-id)
     :acquire-time-ms (parse-rfc3339-ms (:acquireTime spec) :spec.acquireTime)
     :renew-time-ms (parse-rfc3339-ms (:renewTime spec) :spec.renewTime)
     :lease-duration-seconds (parse-positive-int (:leaseDurationSeconds spec) :spec.leaseDurationSeconds)
     :lease-transitions (parse-non-negative-int (:leaseTransitions spec) :spec.leaseTransitions)}))

(defn- expires-at-ms
  [lease]
  (when (and (:renew-time-ms lease) (:lease-duration-seconds lease))
    (+ (:renew-time-ms lease)
       (* 1000 (:lease-duration-seconds lease)))))

(defn- lease-active?
  [lease now-ms]
  (let [expires-at-ms (expires-at-ms lease)]
    (and (:holder-identity lease)
         expires-at-ms
         (< now-ms expires-at-ms))))

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
  (when (lease-active? lease now-ms)
    (common/remaining-ttl-ms (expires-at-ms lease) now-ms)))

(defn- holder-response
  [status lease now-ms]
  [(name status)
   (:holder-identity lease)
   (some-> (:lease-transitions lease) str)
   (some-> (remaining-ttl-ms lease now-ms) str)])

(defn- build-annotations
  [lease token election-id]
  (cond-> (dissoc (or (:annotations lease) {}) token-annotation election-id-annotation)
    token (assoc token-annotation token)
    election-id (assoc election-id-annotation election-id)))

(defn- metadata-for-put
  [namespace lease-name lease annotations]
  (cond-> {:name lease-name
           :namespace namespace
           :resourceVersion (or (:resource-version lease)
                                (throw (ex-info "Kubernetes Lease is missing resourceVersion"
                                                {:type ::invalid-lease-payload
                                                 :field :metadata.resourceVersion})))
           :annotations annotations}
    (seq (:labels lease)) (assoc :labels (:labels lease))))

(defn- create-lease-body
  [namespace lease-name owner-id token election-id now-ms lease-ms]
  {:apiVersion "coordination.k8s.io/v1"
   :kind "Lease"
   :metadata {:name lease-name
              :namespace namespace
              :annotations {token-annotation token
                            election-id-annotation election-id}}
   :spec {:holderIdentity owner-id
          :acquireTime (timestamp now-ms)
          :renewTime (timestamp now-ms)
          :leaseDurationSeconds (lease-duration-seconds lease-ms)
          :leaseTransitions 1}})

(defn- acquire-lease-body
  [namespace lease-name lease owner-id token election-id now-ms lease-ms]
  {:apiVersion "coordination.k8s.io/v1"
   :kind "Lease"
   :metadata (metadata-for-put namespace lease-name lease (build-annotations lease token election-id))
   :spec (-> (:raw-spec lease)
             (assoc :holderIdentity owner-id
                    :acquireTime (timestamp now-ms)
                    :renewTime (timestamp now-ms)
                    :leaseDurationSeconds (lease-duration-seconds lease-ms)
                    :leaseTransitions (inc (long (or (:lease-transitions lease) 0)))))} )

(defn- renew-lease-body
  [namespace lease-name lease owner-id token election-id now-ms lease-ms]
  {:apiVersion "coordination.k8s.io/v1"
   :kind "Lease"
   :metadata (metadata-for-put namespace lease-name lease (build-annotations lease token election-id))
   :spec (-> (:raw-spec lease)
             (assoc :holderIdentity owner-id
                    :acquireTime (or (some-> (:acquire-time-ms lease) timestamp)
                                     (timestamp now-ms))
                    :renewTime (timestamp now-ms)
                    :leaseDurationSeconds (lease-duration-seconds lease-ms)
                    :leaseTransitions (long (or (:lease-transitions lease) 0))))} )

(defn- resign-lease-body
  [namespace lease-name lease election-id]
  {:apiVersion "coordination.k8s.io/v1"
   :kind "Lease"
   :metadata (metadata-for-put namespace lease-name lease (build-annotations lease nil election-id))
   :spec (-> (:raw-spec lease)
             (assoc :holderIdentity nil
                    :acquireTime nil
                    :renewTime nil
                    :leaseDurationSeconds nil
                    :leaseTransitions (long (or (:lease-transitions lease) 0))))} )

(defn- auth-error!
  [response]
  (throw (ex-info "Kubernetes Lease request was not authorized"
                  {:type (if (= 401 (:status response)) ::unauthorized ::forbidden)
                   :backend :kubernetes-lease
                   :status (:status response)
                   :response response
                   :body (:body response)})))

(defn- unsupported-cluster-error!
  [message response]
  (throw (ex-info message
                  {:type ::unsupported-cluster
                   :backend :kubernetes-lease
                   :status (:status response)
                   :response response
                   :body (:body response)})))

(defn- provider-error!
  [message response]
  (throw (ex-info message
                  {:type ::provider-error
                   :backend :kubernetes-lease
                   :status (:status response)
                   :response response
                   :body (:body response)})))

(defn- missing-lease?
  [response lease-name]
  (and (= 404 (:status response))
       (map? (:body response))
       (= "NotFound" (:reason (:body response)))
       (= lease-name (get-in response [:body :details :name]))))

(defn- conflict?
  [response]
  (= 409 (:status response)))

(defn- lease-get-path
  [namespace lease-name]
  (str "/apis/coordination.k8s.io/v1/namespaces/" namespace "/leases/" lease-name))

(defn- lease-post-path
  [namespace]
  (str "/apis/coordination.k8s.io/v1/namespaces/" namespace "/leases"))

(defn- get-lease
  [kubernetes-client namespace lease-name]
  (let [response (kube/request! kubernetes-client
                                {:method :get
                                 :path (lease-get-path namespace lease-name)})]
    (cond
      (= 200 (:status response)) (parse-lease response)
      (missing-lease? response lease-name) nil
      (contains? #{401 403} (:status response)) (auth-error! response)
      (= 404 (:status response)) (unsupported-cluster-error! "Kubernetes Lease API was not found" response)
      :else (provider-error! "Kubernetes Lease GET request failed" response))))

(defn- create-lease!
  [kubernetes-client namespace payload]
  (let [response (kube/request! kubernetes-client
                                {:method :post
                                 :path (lease-post-path namespace)
                                 :headers {"Content-Type" "application/json"}
                                 :body (json/generate-string payload)})]
    (cond
      (contains? #{200 201} (:status response)) (parse-lease response)
      (conflict? response) ::conflict
      (contains? #{401 403} (:status response)) (auth-error! response)
      (= 404 (:status response)) (unsupported-cluster-error! "Kubernetes Lease API was not found" response)
      :else (provider-error! "Kubernetes Lease create request failed" response))))

(defn- replace-lease!
  [kubernetes-client namespace lease-name payload]
  (let [response (kube/request! kubernetes-client
                                {:method :put
                                 :path (lease-get-path namespace lease-name)
                                 :headers {"Content-Type" "application/json"}
                                 :body (json/generate-string payload)})]
    (cond
      (= 200 (:status response)) (parse-lease response)
      (conflict? response) ::conflict
      (contains? #{401 403} (:status response)) (auth-error! response)
      (= 404 (:status response)) (unsupported-cluster-error! "Kubernetes Lease API was not found" response)
      :else (provider-error! "Kubernetes Lease update request failed" response))))

(defn- acquire-busy-result
  [election-id lease now-ms]
  (common/acquire-result :kubernetes-lease election-id
                         [(name :busy)
                          (:holder-identity lease)
                          (some-> (:lease-transitions lease) str)
                          ""
                          (some-> (remaining-ttl-ms lease now-ms) str)]))

(defn- renew-lost-result
  [election-id lease now-ms]
  (if lease
    (common/renew-result :kubernetes-lease election-id (holder-response :lost lease now-ms))
    (common/renew-result :kubernetes-lease election-id ["lost"])))

(defn- resign-not-owner-result
  [election-id lease now-ms]
  (if lease
    (common/resign-result :kubernetes-lease election-id (holder-response :not-owner lease now-ms))
    (common/resign-result :kubernetes-lease election-id ["not-owner"])))

(defrecord KubernetesLeaseLeaderElection [kubernetes-client owner-id namespace lease-name-prefix default-lease-ms clock]
  p/LeaderElectionProtocol
  (acquire! [_ election-id opts]
    (let [election-id (common/normalize-election-id election-id)
          lease-name (lease-name lease-name-prefix election-id)
          lease-ms (common/lease-ms opts default-lease-ms)
          now-ms (common/now-ms clock)
          token (common/generate-token)
          current (get-lease kubernetes-client namespace lease-name)]
      (cond
        (nil? current)
        (let [created (create-lease! kubernetes-client namespace
                                     (create-lease-body namespace lease-name owner-id token election-id now-ms lease-ms))]
          (if (= ::conflict created)
            (if-let [lease (get-lease kubernetes-client namespace lease-name)]
              (acquire-busy-result election-id lease now-ms)
              (common/acquire-result :kubernetes-lease election-id ["busy"]))
            (common/acquire-result :kubernetes-lease election-id
                                   ["acquired"
                                    (:holder-identity created)
                                    (some-> (:lease-transitions created) str)
                                    token
                                    (some-> (remaining-ttl-ms created now-ms) str)])))

        (lease-active? current now-ms)
        (acquire-busy-result election-id current now-ms)

        :else
        (let [updated (replace-lease! kubernetes-client namespace lease-name
                                      (acquire-lease-body namespace lease-name current owner-id token election-id now-ms lease-ms))]
          (if (= ::conflict updated)
            (if-let [lease (get-lease kubernetes-client namespace lease-name)]
              (if (lease-active? lease now-ms)
                (acquire-busy-result election-id lease now-ms)
                (common/acquire-result :kubernetes-lease election-id ["busy"]))
              (common/acquire-result :kubernetes-lease election-id ["busy"]))
            (common/acquire-result :kubernetes-lease election-id
                                   ["acquired"
                                    (:holder-identity updated)
                                    (some-> (:lease-transitions updated) str)
                                    token
                                    (some-> (remaining-ttl-ms updated now-ms) str)]))))))

  (renew! [_ election-id token opts]
    (let [election-id (common/normalize-election-id election-id)
          token (common/normalize-token token)
          lease-name (lease-name lease-name-prefix election-id)
          lease-ms (common/lease-ms opts default-lease-ms)
          now-ms (common/now-ms clock)
          current (get-lease kubernetes-client namespace lease-name)]
      (if (or (nil? current)
              (not (lease-owned-by-token? current token now-ms)))
        (renew-lost-result election-id (when (lease-active? current now-ms) current) now-ms)
        (let [updated (replace-lease! kubernetes-client namespace lease-name
                                      (renew-lease-body namespace lease-name current owner-id token election-id now-ms lease-ms))]
          (if (= ::conflict updated)
            (let [lease (get-lease kubernetes-client namespace lease-name)]
              (if (and lease
                       (lease-owned-by-caller? lease owner-id token now-ms))
                (let [retry (replace-lease! kubernetes-client namespace lease-name
                                            (renew-lease-body namespace lease-name lease owner-id token election-id now-ms lease-ms))]
                  (if (= ::conflict retry)
                    (renew-lost-result election-id (when (lease-active? lease now-ms) lease) now-ms)
                    (common/renew-result :kubernetes-lease election-id
                                         ["renewed"
                                          (:holder-identity retry)
                                          (some-> (:lease-transitions retry) str)
                                          token
                                          (some-> (remaining-ttl-ms retry now-ms) str)])))
                (renew-lost-result election-id (when (lease-active? lease now-ms) lease) now-ms)))
            (common/renew-result :kubernetes-lease election-id
                                 ["renewed"
                                  (:holder-identity updated)
                                  (some-> (:lease-transitions updated) str)
                                  token
                                  (some-> (remaining-ttl-ms updated now-ms) str)]))))))

  (resign! [_ election-id token _opts]
    (let [election-id (common/normalize-election-id election-id)
          token (common/normalize-token token)
          lease-name (lease-name lease-name-prefix election-id)
          now-ms (common/now-ms clock)
          current (get-lease kubernetes-client namespace lease-name)]
      (if (or (nil? current)
              (not (lease-owned-by-token? current token now-ms)))
        (resign-not-owner-result election-id (when (lease-active? current now-ms) current) now-ms)
        (let [updated (replace-lease! kubernetes-client namespace lease-name
                                      (resign-lease-body namespace lease-name current election-id))]
          (if (= ::conflict updated)
            (let [lease (get-lease kubernetes-client namespace lease-name)]
              (if (and lease
                       (lease-owned-by-caller? lease owner-id token now-ms))
                (let [retry (replace-lease! kubernetes-client namespace lease-name
                                            (resign-lease-body namespace lease-name lease election-id))]
                  (if (= ::conflict retry)
                    (resign-not-owner-result election-id (when (lease-active? lease now-ms) lease) now-ms)
                    (common/resign-result :kubernetes-lease election-id
                                          ["released"
                                           owner-id
                                           (some-> (:lease-transitions retry) str)])))
                (resign-not-owner-result election-id (when (lease-active? lease now-ms) lease) now-ms)))
            (common/resign-result :kubernetes-lease election-id
                                  ["released"
                                   owner-id
                                   (some-> (:lease-transitions updated) str)]))))))

  (status [_ election-id _opts]
    (let [election-id (common/normalize-election-id election-id)
          lease-name (lease-name lease-name-prefix election-id)
          now-ms (common/now-ms clock)
          current (get-lease kubernetes-client namespace lease-name)]
      (if (and current (lease-active? current now-ms))
        (common/status-result :kubernetes-lease election-id
                              (holder-response :held current now-ms))
        (common/status-result :kubernetes-lease election-id ["vacant"])))))

(defmethod ig/init-key :d-core.core.leader-election.kubernetes-lease/kubernetes-lease
  [_ {:keys [kubernetes-client owner-id default-lease-ms clock lease-name-prefix]
      :or {default-lease-ms common/default-lease-ms
           lease-name-prefix default-lease-name-prefix}}]
  (when-not kubernetes-client
    (throw (ex-info "Kubernetes Lease leader election requires :kubernetes-client"
                    {:type ::missing-kubernetes-client})))
  (->KubernetesLeaseLeaderElection kubernetes-client
                                   (common/normalize-owner-id owner-id)
                                   (:namespace kubernetes-client)
                                   (normalize-lease-name-prefix lease-name-prefix)
                                   (common/require-positive-long default-lease-ms :default-lease-ms)
                                   (common/normalize-clock clock)))
