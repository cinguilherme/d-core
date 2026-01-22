(ns d-core.dev.datomic-playground
  (:require
   [datomic.api :as d]))

#_(def db-uri "datomic:dev://localhost:5334/hello")

;; this is only working if the transactor is not running, assuming this is basically in local mode
;; datomic has a sotrage datomic and storage admin password, both are set to datomic. So maybe the URI needs to be adjusted?

(def db-uri "datomic:dev://localhost:4334/d-core")


(d/create-database db-uri)

(def db-uri-secure "datomic:dev://localhost:4334/d-core?password=datomic")
(d/create-database db-uri-secure)

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
(d/pull-many (d/db conn) [:user/name] [1 2 3 4])
