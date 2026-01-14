(ns d-core.helpers.logger
  (:require [duct.logger :as logger]))

(defn make-test-logger
  "Returns a logger that records all log calls and the atom of logs."
  []
  (let [logs (atom [])]
    {:logger (reify logger/Logger
               (-log [_ level ns-str file line id event data]
                 (swap! logs conj {:level level :event event :data data})))
     :logs logs}))