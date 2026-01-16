(ns d-core.ws
  (:require
   [aleph.http :as http]
   [integrant.core :as ig]
   [reitit.ring :as ring]))

(defmethod ig/init-key :d-core.ws/handler
  [_ {:keys [routes]}]
  ;; Placeholder for future middleware support (logging, tracing, auth, etc).
  (let [router (ring/router routes)
        not-found (fn [_] {:status 404 :body "Not Found"})]
    (ring/ring-handler router not-found)))

(defmethod ig/init-key :d-core.ws/server
  [_ {:keys [port handler]}]
  (http/start-server handler {:port port}))

(defmethod ig/halt-key! :d-core.ws/server
  [_ server]
  (.close server))
