(ns d-core.core.workers-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.workers]
            [d-core.libs.workers :as workers]
            [integrant.core :as ig]))

(deftest init-key-requires-definition
  (testing "missing definition throws"
    (is (thrown? Exception
                 (ig/init-key :d-core.core.workers/system {})))))

(deftest init-and-halt-workers-system
  (testing "component starts workers and halts cleanly"
    (let [seen (promise)
          definition {:channels {:commands/in {:buffer 1}}
                      :workers {:commands {:kind :command
                                           :in :commands/in
                                           :worker-fn (fn [_ msg] (deliver seen msg))
                                           :dispatch :thread
                                           :expose? true}}}
          system (ig/init-key :d-core.core.workers/system {:definition definition})]
      (try
        (workers/command! system :commands {:cmd :ping})
        (is (= {:cmd :ping} (deref seen 500 ::timeout)))
        (finally
          (ig/halt-key! :d-core.core.workers/system system)
          (let [ch (get-in system [:channels :commands/in])]
            (is (false? (async/offer! ch {:cmd :after-halt})))))))))
