(ns d-core.core.messaging.dead-letter.defaults)

(def ^:dynamic *default-max-attempts*
  "Global default for DLQ max attempts when not specified per-topic.

  Override via JVM system property:
  - `-Ddcore.dlq.max_attempts=N`"
  (let [s (System/getProperty "dcore.dlq.max_attempts")]
    (try
      (long (or (some-> s parse-long) 3))
      (catch Exception _ 3))))

(def ^:dynamic *default-replay-mode*
  "Replay strategy selector.

  - `:sleep` (implemented): sleep `delay-ms` before republishing
  - `:due-time` (future): compute/store next eligible time and skip until due

  Override via JVM system property:
  - `-Ddcore.dlq.replay_mode=sleep`"
  (keyword (or (System/getProperty "dcore.dlq.replay_mode") "sleep")))

