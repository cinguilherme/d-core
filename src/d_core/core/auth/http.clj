(ns d-core.core.auth.http
  (:require
   [duct.logger :as logger]
   [integrant.core :as ig]
   [d-core.core.authn.protocol :as authn]
   [d-core.core.authz.protocol :as authz]))

(defn wrap-authentication
  [handler authenticator {:keys [allow-anonymous?] :as opts}]
  (fn [req]
    (try
      (let [{:keys [principal token]} (authn/authenticate authenticator req opts)
            req (cond-> req
                  principal (assoc :auth/principal principal)
                  token (assoc :auth/token token))]
        (handler req))
      (catch clojure.lang.ExceptionInfo ex
        (if allow-anonymous?
          (handler (assoc req :auth/error (ex-data ex)))
          (authn/challenge authenticator req opts)))
      (catch Exception _ex
        (if allow-anonymous?
          (handler (assoc req :auth/error {:type ::authentication-error}))
          (authn/challenge authenticator req opts))))))

(defn- default-deny-response
  [principal]
  (if principal
    {:status 403 :body "Forbidden"}
    {:status 401 :body "Unauthorized"}))

(defn wrap-authorization
  [handler authorizer {:keys [require-fn on-deny] :as opts}]
  (fn [req]
    (let [principal (:auth/principal req)
          requirement (cond
                        require-fn (require-fn req)
                        (:auth/require req) (:auth/require req)
                        :else nil)]
      (if (nil? requirement)
        (handler req)
        (let [decision (authz/authorize authorizer principal requirement opts)
              req (assoc req :auth/decision decision)]
          (if (:allowed? decision)
            (handler req)
            (if on-deny
              (on-deny req decision)
              (default-deny-response principal))))))))

(defmethod ig/init-key :d-core.core.auth.http/authentication-middleware
  [_ {:keys [authenticator opts logger]}]
  (when logger
    (logger/log logger :info ::authentication-middleware-initialized))
  (fn [handler]
    (wrap-authentication handler authenticator (or opts {}))))

(defmethod ig/init-key :d-core.core.auth.http/authorization-middleware
  [_ {:keys [authorizer opts logger]}]
  (when logger
    (logger/log logger :info ::authorization-middleware-initialized))
  (fn [handler]
    (wrap-authorization handler authorizer (or opts {}))))
