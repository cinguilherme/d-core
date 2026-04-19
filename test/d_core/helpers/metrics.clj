(ns d-core.helpers.metrics
  (:require [d-core.core.metrics.protocol :as metrics]))

(definterface ILabelable
  (labels [^"[Ljava.lang.String;" label-values]))

(defn- make-labelable
  [collector-name]
  (reify ILabelable
    (labels [_ label-values]
      {:collector collector-name
       :labels (vec label-values)})))

(defn make-test-metrics
  []
  (let [calls (atom [])]
    {:calls calls
     :metrics (reify metrics/MetricsProtocol
                (registry [_] nil)
                (counter [_ opts]
                  (make-labelable (:name opts)))
                (gauge [_ opts]
                  (make-labelable (:name opts)))
                (histogram [_ opts]
                  (make-labelable (:name opts)))
                (inc! [_ metric]
                  (swap! calls conj {:op :inc! :metric metric :amount 1}))
                (inc! [_ metric amount]
                  (swap! calls conj {:op :inc! :metric metric :amount amount}))
                (observe! [_ histogram value]
                  (swap! calls conj {:op :observe! :metric histogram :value value})))}))

(defn find-calls
  [calls op & [collector-name]]
  (cond->> @calls
    true (filter #(= op (:op %)))
    collector-name (filter #(= collector-name (get-in % [:metric :collector])))))
