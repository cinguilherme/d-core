(ns d-core.core.clients.kubernetes.client
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [d-core.core.http.client :as http])
  (:import (java.security KeyStore)
           (java.security.cert CertificateFactory)
           (java.util Collection)))

(def default-token-file
  "/var/run/secrets/kubernetes.io/serviceaccount/token")

(def default-ca-cert-file
  "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

(def default-namespace-file
  "/var/run/secrets/kubernetes.io/serviceaccount/namespace")

(def default-request-timeout-ms
  5000)

(defrecord KubernetesClient [http-client api-server-url namespace token-file ca-cert-file namespace-file request-timeout-ms]
  Object
  (toString [_]
    (str "#KubernetesClient{:api-server-url " (pr-str api-server-url)
         ", :namespace " (pr-str namespace) "}")))

(defn env
  [k]
  (System/getenv k))

(defn- require-non-blank
  [value message data]
  (let [value (some-> value str str/trim)]
    (when (str/blank? value)
      (throw (ex-info message data)))
    value))

(defn in-cluster-api-server-url
  []
  (let [host (some-> (env "KUBERNETES_SERVICE_HOST") str/trim)
        port (some-> (or (env "KUBERNETES_SERVICE_PORT_HTTPS")
                         (env "KUBERNETES_SERVICE_PORT")
                         "443")
                     str
                     str/trim)]
    (when (str/blank? host)
      (throw (ex-info "Kubernetes client requires KUBERNETES_SERVICE_HOST or :api-server-url"
                      {:type ::missing-api-server-host})))
    (str "https://" host ":" (if (str/blank? port) "443" port))))

(defn- read-file-trimmed
  [path kind]
  (try
    (require-non-blank (slurp path)
                       "Kubernetes client requires a non-blank file value"
                       {:type ::blank-file-value
                        :kind kind
                        :path path})
    (catch Exception ex
      (throw (ex-info "Failed to read Kubernetes client file"
                      {:type ::file-read-failed
                       :kind kind
                       :path path}
                      ex)))))

(defn load-trust-store
  [ca-cert-file]
  (with-open [in (io/input-stream ca-cert-file)]
    (let [factory (CertificateFactory/getInstance "X.509")
          ^Collection certs (.generateCertificates factory in)
          ks (KeyStore/getInstance (KeyStore/getDefaultType))]
      (.load ks nil nil)
      (when (zero? (.size certs))
        (throw (ex-info "Kubernetes CA certificate file did not contain any certificates"
                        {:type ::missing-certificates
                         :path ca-cert-file})))
      (doseq [[idx cert] (map-indexed vector certs)]
        (.setCertificateEntry ks (str "kubernetes-ca-" idx) cert))
      ks)))

(defn- normalize-timeout-ms
  [timeout-ms]
  (let [value (long (or timeout-ms default-request-timeout-ms))]
    (when (<= value 0)
      (throw (ex-info "Kubernetes client :request-timeout-ms must be greater than zero"
                      {:type ::invalid-timeout
                       :request-timeout-ms timeout-ms})))
    value))

(defn- parse-json-body
  [body]
  (cond
    (map? body) body
    (string? body) (if (str/blank? body)
                     nil
                     (try
                       (json/parse-string body true)
                       (catch Exception _
                         body)))
    :else body))

(defn make-client
  [{:keys [api-server-url token-file ca-cert-file namespace-file namespace request-timeout-ms]
    :or {token-file default-token-file
         ca-cert-file default-ca-cert-file
         namespace-file default-namespace-file
         request-timeout-ms default-request-timeout-ms}}]
  (let [api-server-url (or api-server-url (in-cluster-api-server-url))
        namespace (or namespace (read-file-trimmed namespace-file :namespace))
        timeout-ms (normalize-timeout-ms request-timeout-ms)
        trust-store (load-trust-store ca-cert-file)
        http-client (http/make-client {:id :kubernetes
                                       :base-url api-server-url
                                       :http-opts {:trust-store trust-store
                                                   :conn-timeout timeout-ms
                                                   :socket-timeout timeout-ms}})]
    (->KubernetesClient http-client
                        api-server-url
                        namespace
                        token-file
                        ca-cert-file
                        namespace-file
                        timeout-ms)))

(defn request!
  [client {:keys [headers] :as request}]
  (let [token (read-file-trimmed (:token-file client) :token)
        response (http/request! (:http-client client)
                                (-> request
                                    (update :headers merge
                                            {"Accept" "application/json"
                                             "Authorization" (str "Bearer " token)})
                                    (update :as #(or % :text))))]
    (update response :body parse-json-body)))
