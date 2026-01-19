(ns d-core.core.messaging.deferred.scheduler-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [d-core.core.messaging.deferred.protocol :as deferred]
            [d-core.core.producers.protocol :as p]
            [d-core.core.messaging.deferred.scheduler]))

(defn- promise-producer
  [deliver]
  (reify p/Producer
    (produce! [_ msg options]
      (deliver {:msg msg :options options})
      {:ok true})))

(deftest deferred-scheduler-fires-job
  (testing "scheduled message is produced at (or after) deliver-at-ms"
    (let [delivered (promise)
          producer (promise-producer #(deliver delivered %))
          scheduler (ig/init-key :d-core.core.messaging.deferred/scheduler
                                 {:producers {:kafka producer}
                                  :start? true
                                  :require-jdbc? false})]
      (try
        (let [deliver-at-ms (+ (System/currentTimeMillis) 200)
              res (deferred/schedule! scheduler
                                      {:producer :kafka
                                       :msg {:id 1}
                                       :options {:topic :orders}
                                       :deliver-at-ms deliver-at-ms})
              result (deref delivered 2000 ::timeout)]
          (is (= {:ok true :status :scheduled}
                 (select-keys res [:ok :status])))
          (is (not= ::timeout result))
          (is (= {:msg {:id 1} :options {:topic :orders}}
                 result)))
        (finally
          (ig/halt-key! :d-core.core.messaging.deferred/scheduler scheduler))))))

(deftest deferred-scheduler-requires-deliver-at-ms
  (testing "missing deliver-at-ms throws"
    (let [scheduler (ig/init-key :d-core.core.messaging.deferred/scheduler
                                 {:producers {}
                                  :start? false
                                  :require-jdbc? false})]
      (try
        (is (thrown? clojure.lang.ExceptionInfo
                     (deferred/schedule! scheduler
                                         {:producer :kafka
                                          :msg {:id 1}
                                          :options {:topic :orders}})))
        (finally
          (ig/halt-key! :d-core.core.messaging.deferred/scheduler scheduler))))))

(deftest deferred-scheduler-requires-jdbc-jobstore
  (testing "mounting without JDBCJobStore config throws by default"
    (is (thrown? clojure.lang.ExceptionInfo
                 (ig/init-key :d-core.core.messaging.deferred/scheduler
                              {:producers {}
                               :start? false})))))
