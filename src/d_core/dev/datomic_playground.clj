(ns d-core.dev.datomic-playground
  (:require
   [datomic.api :as d]))

(def db-uri "datomic:dev://localhost:4334/hello")

(d/create-database db-uri)

(def conn (d/connect db-uri))

(d/transact conn [{:db/id #db/id[:db.part/user]
                   :db/ident :user/name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}])


(d/transact conn [{:db/id 1
                   :user/name "John"
                   }
                  {:db/id 2
                   :user/name "Jane"
                   }])

(d/q '[:find ?e :where [?e :user/name]] (d/db conn))
(d/pull (d/db conn) [:user/name] 1)
(d/pull-many (d/db conn) [:user/name] [1 2])