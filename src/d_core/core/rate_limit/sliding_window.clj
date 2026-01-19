(ns d-core.core.rate-limit.sliding-window
  (:require [integrant.core :as ig]
            [d-core.core.rate-limit.protocol :as p]))

(defn- now-ms [clock]
  (long (clock)))

(defn- prune-timestamps [timestamps now window-ms]
  (let [cutoff (- now window-ms)]
    (->> timestamps
         (filter #(> % cutoff))
         (vec))))

(defn- consume-window
  [{:keys [state limit window-ms clock]} key opts]
  (let [{:keys [amount]} opts
        amount (long (or amount 1))
        now (now-ms clock)
        result (volatile! nil)]
    (if (> amount limit)
      {:allowed? false :remaining 0 :reset-at nil :retry-after-ms nil}
      (do
        (swap! state
               (fn [m]
                 (let [timestamps (get m key [])
                       kept (prune-timestamps timestamps now window-ms)
                       available (- limit (count kept))
                       allowed? (<= amount available)
                       new-ts (if allowed?
                                (into kept (repeat amount now))
                                kept)
                       remaining (max 0 (- limit (count new-ts)))
                       reset-at (if (seq new-ts)
                                  (+ (first new-ts) window-ms)
                                  (+ now window-ms))
                       retry-after-ms (when-not allowed?
                                        (max 0 (- reset-at now)))
                       res {:allowed? allowed?
                            :remaining remaining
                            :reset-at reset-at
                            :retry-after-ms retry-after-ms}]
                   (vreset! result res)
                   (if (seq new-ts)
                     (assoc m key new-ts)
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

(defrecord SlidingWindowRateLimiter [limit window-ms clock state]
  p/RateLimitProtocol
  (consume! [_ key opts]
    (consume-window {:state state
                     :limit (long (or (:limit opts) limit))
                     :window-ms (long (or (:window-ms opts) window-ms))
                     :clock clock}
                    key opts))
  (consume-blocking! [this key opts]
    (blocking-consume this key opts)))

(defmethod ig/init-key :d-core.core.rate-limit.sliding-window/limiter
  [_ {:keys [limit window-ms clock]
      :or {limit 100 window-ms 60000 clock #(System/currentTimeMillis)}}]
  (->SlidingWindowRateLimiter (long limit) (long window-ms) clock (atom {})))
