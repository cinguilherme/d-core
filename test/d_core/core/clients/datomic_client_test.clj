(ns d-core.core.clients.datomic-client-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.datomic.client :as datomic-client]))

(deftest make-client-requires-datomic-deps
  (testing "make-client fails fast when Datomic API is not on classpath"
    (let [datomic-available? (try
                               (some? (requiring-resolve 'datomic.api/connect))
                               (catch Throwable _ false))]
      (if datomic-available?
        (is true "Datomic on classpath; skipping optional-dep test")
        (try
          (datomic-client/make-client {:uri "datomic:dev://localhost:4334/test"})
          (is false "Expected make-client to throw without Datomic dependency")
          (catch clojure.lang.ExceptionInfo ex
            (is (= 'datomic.api/create-database (:symbol (ex-data ex))))))))))
