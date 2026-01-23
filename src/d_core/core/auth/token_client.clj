(ns d-core.core.auth.token-client
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [duct.logger :as logger]
   [integrant.core :as ig]))

(defrecord TokenClient [token-url client-id client-secret http-opts logger])

(defn make-client
  [{:keys [token-url client-id client-secret http-opts logger]}]
  (when-not token-url
    (throw (ex-info "token-url is required" {:type ::missing-token-url})))
  (->TokenClient token-url client-id client-secret http-opts logger))

(defn- parse-token-response
  [resp]
  (let [status (:status resp)
        parsed (json/parse-string (:body resp) true)]
    (if (<= 200 status 299)
      {:access-token (:access_token parsed)
       :refresh-token (:refresh_token parsed)
       :expires-in (:expires_in parsed)
       :token-type (:token_type parsed)
       :scope (:scope parsed)
       :raw parsed}
      (throw (ex-info "Token request failed"
                      {:type ::token-request-failed
                       :status status
                       :body parsed})))))

(defn- request-token
  [{:keys [token-url client-id client-secret http-opts]} params]
  (let [params (cond-> params
                 client-id (assoc :client_id client-id)
                 client-secret (assoc :client_secret client-secret))
        resp (http/post token-url (merge {:form-params params
                                          :as :text
                                          :throw-exceptions false}
                                         http-opts))]
    (parse-token-response resp)))

(defn client-credentials
  [client {:keys [scope audience]}]
  (request-token client (cond-> {:grant_type "client_credentials"}
                          scope (assoc :scope scope)
                          audience (assoc :audience audience))))

(defn token-exchange
  [client {:keys [subject-token subject-token-type actor-token actor-token-type
                  requested-token-type audience scope]}]
  (request-token client (cond-> {:grant_type "urn:ietf:params:oauth:grant-type:token-exchange"
                                 :subject_token subject-token
                                 :subject_token_type (or subject-token-type
                                                         "urn:ietf:params:oauth:token-type:access_token")
                                 :requested_token_type (or requested-token-type
                                                           "urn:ietf:params:oauth:token-type:access_token")}
                          actor-token (assoc :actor_token actor-token)
                          actor-token-type (assoc :actor_token_type actor-token-type)
                          audience (assoc :audience audience)
                          scope (assoc :scope scope))))

(defmethod ig/init-key :d-core.core.auth/token-client
  [_ opts]
  (when-let [logger (:logger opts)]
    (logger/log logger :info ::token-client-initialized
                {:token-url (:token-url opts)}))
  (make-client opts))
