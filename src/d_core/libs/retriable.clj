(ns d-core.libs.retriable
  (:refer-clojure :exclude []))

(defmacro retriable
  "Runs `retriable-body`, retrying on failures up to `max-attempts` total attempts.

  Contracts:
  - `max-attempts` is the total number of attempts (includes the initial run).
  - `retriable-errors` is a collection of Exception classes to retry on
    (checked via `instance?`, so subclasses match).
  - `backoff-strategy` is a fn that receives a 1-based failure count and returns
    the number of milliseconds to sleep before the next attempt.

  If the thrown exception is not retriable, or we have reached `max-attempts`,
  the exception is rethrown."
  [max-attempts backoff-strategy retriable-errors & retriable-body]
  `(let [max-attempts# ~max-attempts
         backoff-strategy# ~backoff-strategy
         retriable-errors# ~retriable-errors
         ok-sentinel# (Object.)]
     (loop [attempt# 1]
       (let [result# (try
                      [ok-sentinel# (do ~@retriable-body)]
                      (catch Exception e#
                        e#))]
         (if (and (vector? result#) (identical? ok-sentinel# (first result#)))
           (second result#)
           (let [e# result#
                 retriable?# (boolean (some #(instance? % e#) retriable-errors#))
                 can-retry?# (and retriable?# (< attempt# max-attempts#))]
             (if can-retry?#
               (let [delay-ms# (backoff-strategy# attempt#)
                     delay-ms# (max 0 (long (or delay-ms# 0)))]
                 (Thread/sleep delay-ms#)
                 (recur (inc attempt#)))
               (throw e#))))))))