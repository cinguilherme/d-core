(ns d-core.core.databases.protocols.datomic)

(defprotocol DatomicProtocol
  (conn [_]
    "Returns the underlying Datomic connection.")
  (db [_]
    "Returns the current Datomic database value.")
  (transact! [_ tx-data opts]
    "Transacts tx-data. opts may include :async? to return the future.")
  (q [_ query inputs opts]
    "Runs a Datomic query against the current db.")
  (pull [_ pattern eid opts]
    "Runs a Datomic pull against the current db.")
  (pull-many [_ pattern eids opts]
    "Runs a Datomic pull-many against the current db."))
