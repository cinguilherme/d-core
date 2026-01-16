(ns d-core.core.http
  (:require
   [integrant.core :as ig]
   [d-core.core.http.client :as client]))

(defmethod ig/init-key :d-core.core.http/client
  [_ opts]
  (client/make-client opts))

(defmethod ig/init-key :d-core.core.http/clients
  [_ {:keys [clients] :as opts}]
  (let [clients (or clients opts)]
    (into {}
          (for [[k v] clients]
            [k (client/make-client v)]))))
