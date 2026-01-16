(ns d-core.ws
  (:require
   [aleph.http :as http]
   [duct.logger :as logger]
   [integrant.core :as ig]
   [reitit.ring :as ring]))

(defmethod ig/init-key :d-core.ws/handler
  [_ {:keys [routes logger]}]
  ;; Placeholder for future middleware support (logging, tracing, auth, etc).
  (when logger
    (logger/log logger :info ::ws-handler-initialized
                {:routes (count routes)}))
  (let [router (ring/router routes)
        not-found (fn [_] {:status 404 :body "Not Found"})]
    (ring/ring-handler router not-found)))

(defmethod ig/init-key :d-core.ws/server
  [_ {:keys [port handler logger]}]
  (when logger
    (logger/log logger :info ::ws-server-starting {:port port}))
  {:server (http/start-server handler {:port port})
   :port port
   :logger logger})

(defmethod ig/halt-key! :d-core.ws/server
  [_ server]
  (let [{:keys [server logger port]} (if (map? server) server {:server server})]
    (when logger
      (logger/log logger :info ::ws-server-stopping
                  (cond-> {} port (assoc :port port))))
    (.close server)))
