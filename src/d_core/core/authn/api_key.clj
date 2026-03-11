(ns d-core.core.authn.api-key
  (:require [clojure.string :as str]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [d-core.core.api-keys.protocol :as api-keys]
            [d-core.core.authn.protocol :as p]))

(defn- header-token
  [headers]
  (or (get headers "x-api-key")
      (when-let [auth (get headers "authorization")]
        (let [auth (str/trim auth)
              lower (str/lower-case auth)]
          (cond
            (str/starts-with? lower "apikey ") (str/trim (subs auth 7))
            (str/starts-with? lower "api-key ") (str/trim (subs auth 8))
            :else nil)))))

(defn- token-from-request
  [request opts]
  (or (:token opts)
      (header-token (:headers request))))

(defn- ->principal
  [api-key]
  {:subject (:api-key-id api-key)
   :tenant-id (:tenant-id api-key)
   :scopes (set (:scopes api-key))
   :auth-type :api-key
   :api-key/id (:api-key-id api-key)
   :api-key/name (:name api-key)
   :api-key/limits (:limits api-key)
   :api-key/metadata (:metadata api-key)
   :api-key/expires-at (:expires-at api-key)})

(defrecord ApiKeyAuthenticator [api-key-store realm logger]
  p/Authenticator
  (authenticate [_ request opts]
    (let [token (token-from-request request opts)]
      (when-not (seq token)
        (throw (ex-info "Missing API key"
                        {:type ::missing-api-key})))
      (if-let [api-key (api-keys/authenticate-key api-key-store token opts)]
        {:principal (->principal api-key)
         :token token}
        (throw (ex-info "Invalid API key"
                        {:type ::invalid-api-key})))))

  (verify-token [_ token opts]
    (when-not (seq token)
      (throw (ex-info "Missing API key"
                      {:type ::missing-api-key})))
    (if-let [api-key (api-keys/authenticate-key api-key-store token opts)]
      (->principal api-key)
      (throw (ex-info "Invalid API key"
                      {:type ::invalid-api-key}))))

  (challenge [_ request _opts]
    (let [host (get-in request [:headers "host"])
          realm (or realm host "d-core")]
      {:status 401
       :headers {"WWW-Authenticate" (str "ApiKey realm=\"" realm "\"")}
       :body "Unauthorized"})))

(defmethod ig/init-key :d-core.core.authn.api-key/authenticator
  [_ {:keys [api-key-store realm logger]}]
  (when-not api-key-store
    (throw (ex-info "API key authenticator requires :api-key-store"
                    {:type ::missing-api-key-store})))
  (when logger
    (logger/log logger :info ::api-key-authenticator-initialized {:realm realm}))
  (->ApiKeyAuthenticator api-key-store realm logger))
