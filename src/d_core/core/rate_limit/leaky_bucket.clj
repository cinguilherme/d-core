(ns d-core.core.rate-limit.leaky-bucket
  (:require [integrant.core :as ig]
            [d-core.core.rate-limit.protocol :as p]))

(defn- now-ms [clock]
  (long (clock)))

(defn- drain-level
  [level last-ms now leak-rate-per-ms]
  (let [elapsed (max 0 (- now last-ms))
        leaked (* leak-rate-per-ms elapsed)]
    (max 0.0 (- level leaked))))

(defn- retry-after-ms
  [level capacity leak-rate-per-ms]
  (when (pos? leak-rate-per-ms)
    (let [excess (- level capacity)]
      (when (pos? excess)
        (long (Math/ceil (/ excess leak-rate-per-ms)))))))

(defn- bucket-reset-at
  [now level leak-rate-per-ms]
  (when (pos? leak-rate-per-ms)
    (+ now (long (Math/ceil (/ level leak-rate-per-ms))))))

(defn- consume-bucket
  [{:keys [state capacity leak-rate-per-ms clock]} key opts]
  (let [amount (double (or (:amount opts) 1))
        now (now-ms clock)
        result (volatile! nil)]
    (if (> amount capacity)
      {:allowed? false :remaining 0 :reset-at nil :retry-after-ms nil}
      (do
        (swap! state
               (fn [m]
                 (let [{:keys [level last-ms]} (get m key {:level 0.0 :last-ms now})
                       drained (drain-level level last-ms now leak-rate-per-ms)
                       next-level (if (<= (+ drained amount) capacity)
                                    (+ drained amount)
                                    drained)
                       allowed? (<= (+ drained amount) capacity)
                       remaining (max 0.0 (- capacity next-level))
                       reset-at (bucket-reset-at now next-level leak-rate-per-ms)
                       retry-ms (when-not allowed?
                                  (retry-after-ms (+ drained amount) capacity leak-rate-per-ms))
                       res {:allowed? allowed?
                            :remaining (long remaining)
                            :reset-at reset-at
                            :retry-after-ms retry-ms}
                       new-entry (when (pos? next-level)
                                   {:level next-level :last-ms now})]
                   (vreset! result res)
                   (if new-entry
                     (assoc m key new-entry)
                     (dissoc m key)))))
        @result))))

(defn- blocking-consume
  [limiter key opts]
  (loop []
    (let [res (p/consume! limiter key opts)]
      (if (:allowed? res)
        res
        (if-let [retry-ms (:retry-after-ms res)]
          (do
            (Thread/sleep (max 0 (long retry-ms)))
            (recur))
          res)))))

(defrecord LeakyBucketRateLimiter [capacity leak-rate-per-ms clock state]
  p/RateLimitProtocol
  (consume! [_ key opts]
    (consume-bucket {:state state
                     :capacity (double (or (:capacity opts) capacity))
                     :leak-rate-per-ms (double (or (:leak-rate-per-ms opts) leak-rate-per-ms))
                     :clock clock}
                    key opts))
  (consume-blocking! [this key opts]
    (blocking-consume this key opts)))

(defmethod ig/init-key :d-core.core.rate-limit.leaky-bucket/limiter
  [_ {:keys [capacity leak-rate-per-sec clock]
      :or {capacity 100 leak-rate-per-sec 1.0 clock #(System/currentTimeMillis)}}]
  (->LeakyBucketRateLimiter (double capacity)
                            (double (/ leak-rate-per-sec 1000.0))
                            clock
                            (atom {})))
