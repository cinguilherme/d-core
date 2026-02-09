(ns d-core.module.ws
  (:require
   [clojure.walk :as walk]
   [integrant.core :as ig]))

(def ^:private request-methods
  #{:get :head :patch :delete :options :post :put :trace})

(defn- endpoint-map?
  [m]
  (or (contains? m :handler)
      (some #(contains? m %) request-methods)))

(defn- normalize-handler
  [handler]
  (cond
    (qualified-keyword? handler) (ig/ref handler)
    (and (map? handler) (qualified-keyword? (:handler handler)))
    (update handler :handler ig/ref)
    :else handler))

(defn- update-endpoint
  [m]
  (let [m (if (qualified-keyword? (:handler m))
            (update m :handler ig/ref)
            m)]
    (reduce-kv
      (fn [acc k v]
        (if (request-methods k)
          (assoc acc k (normalize-handler v))
          (assoc acc k v)))
      m
      m)))

(defn- add-refs-to-routes
  [routes]
  (walk/postwalk
    (fn [x]
      (if (and (map? x) (endpoint-map? x))
        (update-endpoint x)
        x))
    routes))

(defmethod ig/expand-key :d-core.module/ws
  [_ {:keys [routes port middleware logger]
      :or {routes []}}]
  (let [routes (add-refs-to-routes routes)]
    `{:d-core.ws/handler
      {:routes ~routes
       :middleware ~middleware
       :logger ~logger}

      :d-core.ws/server
      {:port ~port
       :handler ~(ig/ref :d-core.ws/handler)
       :logger ~logger}}))
