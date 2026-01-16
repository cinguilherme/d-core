(ns d-core.core.databases.datomic-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.databases.datomic :as datomic-db]
            [d-core.core.databases.protocols.datomic :as p]))

(defn- immediate-deref
  [value]
  (reify clojure.lang.IDeref
    (deref [_] value)))

(deftest datomic-database-delegates-to-api
  (testing "protocol methods call into provided Datomic API"
    (let [calls (atom [])
          conn ::conn
          api {:db (fn [c]
                     (swap! calls conj [:db c])
                     :db-val)
               :q (fn [query db & inputs]
                    (swap! calls conj [:q query db inputs])
                    :q-result)
               :pull (fn [db pattern eid]
                       (swap! calls conj [:pull db pattern eid])
                       {:pulled eid})
               :pull-many (fn [db pattern eids]
                            (swap! calls conj [:pull-many db pattern eids])
                            (mapv (fn [eid] {:pulled eid}) eids))
               :transact (fn [c tx-data]
                           (swap! calls conj [:transact c tx-data])
                           (immediate-deref {:tx-data tx-data}))}
          db (datomic-db/->DatomicDatabase conn api nil)]
      (is (= :db-val (p/db db)))
      (is (= :q-result (p/q db :query [:input] {})))
      (is (= {:pulled 42} (p/pull db '[:user/name] 42 {})))
      (is (= [{:pulled 1} {:pulled 2}] (p/pull-many db '[:user/name] [1 2] {})))
      (is (= {:tx-data [{:foo "bar"}]} (p/transact! db [{:foo "bar"}] {})))
      (let [async-result (p/transact! db [] {:async? true})]
        (is (instance? clojure.lang.IDeref async-result))
        (is (= {:tx-data []} @async-result)))
      (is (= [[:db ::conn]
              [:q :query :db-val '(:input)]
              [:pull :db-val '[:user/name] 42]
              [:pull-many :db-val '[:user/name] [1 2]]
              [:transact ::conn [{:foo "bar"}]]
              [:transact ::conn []]]
             @calls)))))
