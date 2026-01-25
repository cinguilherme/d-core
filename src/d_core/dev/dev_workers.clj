(ns d-core.dev.dev-workers
  (:require [clojure.core.async :as async]))

(def clock-tick (async/chan 1))


(defn clock-worker
  [tick-chan]
  (async/go-loop []
    (let [tick (async/<! tick-chan)]
      (println "Clock tick:" tick)
      (async/put! tick-chan (inc tick))
      (recur))))

(defn start-clock-worker
  []
  (async/go
    (clock-worker clock-tick)))

(def clock-worker-thread (async/go (start-clock-worker)))