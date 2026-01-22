(ns d-core.core.authz.scope
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [duct.logger :as logger]
   [integrant.core :as ig]
   [d-core.core.authz.protocol :as p]))

(defn- normalize-scope-set
  [scopes]
  (cond
    (nil? scopes) #{}
    (set? scopes) scopes
    (string? scopes) (->> (str/split scopes #"\s+") (remove str/blank?) set)
    (coll? scopes) (->> scopes (map str) set)
    :else #{(str scopes)}))

(defn- normalize-tenant
  [tenant]
  (when tenant
    (if (keyword? tenant) (name tenant) (str tenant))))

(defrecord ScopeAuthorizer [logger]
  p/Authorizer
  (authorize [_ principal {:keys [tenant scopes]} _opts]
    (let [required-tenant (normalize-tenant tenant)
          required-scopes (normalize-scope-set scopes)
          principal-tenant (some-> (:tenant-id principal) normalize-tenant)
          principal-scopes (normalize-scope-set (:scopes principal))]
      (cond
        (nil? principal)
        {:allowed? false :reason :unauthenticated}

        (and required-tenant (not= required-tenant principal-tenant))
        {:allowed? false
         :reason :tenant-mismatch
         :details {:required required-tenant
                   :actual principal-tenant}}

        (not (set/subset? required-scopes principal-scopes))
        {:allowed? false
         :reason :missing-scope
         :details {:required required-scopes
                   :actual principal-scopes}}

        :else
        {:allowed? true}))))

(defmethod ig/init-key :d-core.core.authz.scope/authorizer
  [_ {:keys [logger]}]
  (when logger
    (logger/log logger :info ::scope-authorizer-initialized))
  (->ScopeAuthorizer logger))
