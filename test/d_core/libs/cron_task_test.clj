(ns d-core.libs.cron-task-test
  (:require [clojure.test :refer :all]
            [d-core.libs.cron-task :as cron]))

(deftest normalize-tasks-from-map
  (let [tasks (#'cron/normalize-tasks
               {:cleanup {:cron "0 0 * * * ?" :handler :cleanup}})
        task (first tasks)]
    (is (= 1 (count tasks)))
    (is (= :cleanup (:id task)))
    (is (= true (:enabled task)))))

(deftest normalize-tasks-from-vector
  (let [tasks (#'cron/normalize-tasks
               [{:id :sync :cron "0 */5 * * * ?" :handler :sync}])
        task (first tasks)]
    (is (= 1 (count tasks)))
    (is (= :sync (:id task)))
    (is (= true (:enabled task)))))

(deftest validate-task-happy-path
  (let [task {:id :cleanup :cron "0 0 * * * ?" :handler :cleanup}]
    (is (= task (#'cron/validate-task! task {:cleanup (fn [_])})))))

(deftest validate-task-invalid-cron
  (is (thrown? clojure.lang.ExceptionInfo
               (#'cron/validate-task! {:id :bad :cron "nope" :handler :x}
                                      {:x (fn [_])}))))

(deftest validate-task-missing-handler
  (is (thrown? clojure.lang.ExceptionInfo
               (#'cron/validate-task! {:id :bad :cron "0 0 * * * ?" :handler :missing}
                                      {}))))

(deftest validate-task-invalid-misfire
  (is (thrown? clojure.lang.ExceptionInfo
               (#'cron/validate-task! {:id :bad :cron "0 0 * * * ?"
                                       :handler :x :misfire :nope}
                                      {:x (fn [_])}))))
