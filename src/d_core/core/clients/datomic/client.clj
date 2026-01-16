(ns d-core.core.clients.datomic.client)

(defrecord DatomicClient [uri conn api]
  Object
  (toString [_] (str "#DatomicClient{:uri " (pr-str uri) "}")))

(defn- resolve-api
  [sym]
  (or (requiring-resolve sym)
      (throw (ex-info "Datomic dependency missing. Add com.datomic/datomic-free or com.datomic/datomic-pro."
                      {:symbol sym}))))

(defn- datomic-api
  []
  {:create-database (resolve-api 'datomic.api/create-database)
   :delete-database (resolve-api 'datomic.api/delete-database)
   :connect (resolve-api 'datomic.api/connect)
   :release (resolve-api 'datomic.api/release)
   :db (resolve-api 'datomic.api/db)
   :q (resolve-api 'datomic.api/q)
   :pull (resolve-api 'datomic.api/pull)
   :pull-many (resolve-api 'datomic.api/pull-many)
   :transact (resolve-api 'datomic.api/transact)})

(defn make-client
  "Builds a Datomic client from a peer URI.

  opts:
  - :uri (default datomic:dev://localhost:4334/d-core)
  - :create? (default true)"
  [{:keys [uri create?]
    :or {uri "datomic:dev://localhost:4334/d-core"
         create? true}}]
  (let [api (datomic-api)]
    (when create?
      ((:create-database api) uri))
    (->DatomicClient uri ((:connect api) uri) api)))

(defn close!
  "Releases a Datomic connection."
  [client]
  (when (and client (:conn client) (get-in client [:api :release]))
    ((get-in client [:api :release]) (:conn client))))
