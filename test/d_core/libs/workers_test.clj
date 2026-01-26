(ns d-core.libs.workers-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [d-core.libs.workers :as workers]))

(defn await-value
  [ch ms]
  (let [[v port] (async/alts!! [ch (async/timeout ms)])]
    (if (= port ch)
      v
      ::timeout)))

(deftest command-drop-records-stats-and-emits
  (testing "drops on input channel record stats and emit events"
    (let [events (atom [])
          started (promise)
          latch (promise)
          definition {:channels {:commands/in {:buffer 1 :buffer-type :fixed :put-mode :drop}}
                      :workers {:commands {:kind :command
                                           :in :commands/in
                                           :worker-fn (fn [_ _]
                                                        (deliver started true)
                                                        @latch)
                                           :dispatch :thread
                                           :expose? true}}}
          components {:observability {:emit #(swap! events conj %)}}
          system (workers/start-workers definition components)]
      (try
        (testing "first message is accepted and blocks worker"
          (workers/command! system :commands {:n 1})
          (is (true? (deref started 500 false))))
        (testing "subsequent messages are dropped"
          (workers/command! system :commands {:n 2})
          (workers/command! system :commands {:n 3})
          (async/<!! (async/timeout 20))
          (is (= 1 (get-in (workers/stats-snapshot system) [:drops :commands/in])))
          (is (= :workers.drop (:event/type (first @events))))
          (is (= :input-drop (:reason (first @events)))))
        (finally
          (deliver latch true)
          ((:stop! system)))))))

(deftest output-drop-records-stats
  (testing "output channel drops are recorded"
    (let [events (atom [])
          definition {:channels {:commands/in {:buffer 1}
                                 :out {:buffer 0}}
                      :workers {:commands {:kind :command
                                           :in :commands/in
                                           :worker-fn (fn [_ _] :ok)
                                           :output-chan :out
                                           :dispatch :thread
                                           :expose? true}}}
          components {:observability {:emit #(swap! events conj %)}}
          system (workers/start-workers definition components)]
      (try
        (workers/command! system :commands {:n 1})
        (async/<!! (async/timeout 20))
        (is (= 1 (get-in (workers/stats-snapshot system) [:drops :out])))
        (is (= :output-drop (:reason (first @events))))
        (finally
          ((:stop! system)))))))

(deftest fail-chan-receives-error
  (testing "worker exceptions are routed to fail-chan"
    (let [definition {:channels {:commands/in {:buffer 1}
                                 :workers/errors {:buffer 1}}
                      :workers {:commands {:kind :command
                                           :in :commands/in
                                           :worker-fn (fn [_ _] (throw (ex-info "boom" {:kind :test})))
                                           :dispatch :thread
                                           :fail-chan :workers/errors
                                           :expose? true}}}
          system (workers/start-workers definition {})]
      (try
        (workers/command! system :commands {:n 1})
        (let [event (await-value (get-in system [:channels :workers/errors]) 200)]
          (is (not= ::timeout event))
          (is (= :commands (:worker-id event)))
          (is (= :workers/errors (:fail-chan event))))
        (is (= 1 (get-in (workers/stats-snapshot system) [:errors :commands])))
        (finally
          ((:stop! system)))))))

(deftest ticker-drop-records-stats
  (testing "ticker drops are recorded when output is full"
    (let [definition {:channels {:ticks {:buffer 1}}
                      :workers {:clock {:kind :ticker
                                        :interval-ms 5
                                        :out :ticks}}}
          system (workers/start-workers definition {})]
      (try
        (async/<!! (async/timeout 30))
        (is (pos? (get-in (workers/stats-snapshot system) [:drops :ticks] 0)))
        (finally
          ((:stop! system)))))))

(deftest go-guard-timeout-routes-to-fail-chan
  (testing "go-guard timeouts are treated as worker errors"
    (let [definition {:channels {:commands/in {:buffer 1}
                                 :workers/errors {:buffer 1}}
                      :workers {:commands {:kind :command
                                           :in :commands/in
                                           :worker-fn (fn [_ _] (Thread/sleep 50))
                                           :dispatch :go
                                           :fail-chan :workers/errors
                                           :expose? true}}}
          system (workers/start-workers definition {} {:dev-guard? true :guard-ms 5})]
      (try
        (workers/command! system :commands {:n 1})
        (let [event (await-value (get-in system [:channels :workers/errors]) 200)]
          (is (not= ::timeout event))
          (is (= :commands (:worker-id event))))
        (is (= 1 (get-in (workers/stats-snapshot system) [:errors :commands])))
        (finally
          ((:stop! system)))))))
