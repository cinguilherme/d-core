(ns d-core.core.databases.datomic
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.databases.protocols.datomic :as p]))

(defn- log*
  [maybe-logger level event data]
  (when maybe-logger
    (logger/log maybe-logger level event data)))

(defrecord DatomicDatabase [conn api logger]
  p/DatomicProtocol
  (conn [_] conn)
  (db [_] ((:db api) conn))
  (transact! [this tx-data {:keys [async?] :or {async? false}}]
    (log* logger :debug ::transact! {:tx-count (count tx-data)})
    (let [f ((:transact api) (p/conn this) tx-data)]
      (if async?
        f
        @f)))
  (q [this query inputs _opts]
    (log* logger :debug ::q {:inputs (count inputs)})
    (apply (:q api) query (p/db this) inputs))
  (pull [this pattern eid _opts]
    ((:pull api) (p/db this) pattern eid))
  (pull-many [this pattern eids _opts]
    ((:pull-many api) (p/db this) pattern eids)))

(defmethod ig/init-key :d-core.core.databases.datomic/db
  [_ {:keys [datomic-client logger] :as opts}]
  (when-not datomic-client
    (throw (ex-info "Datomic DB component requires :datomic-client" {:opts opts})))
  (->DatomicDatabase (:conn datomic-client) (:api datomic-client) logger))
