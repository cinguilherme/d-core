(ns d-core.core.messaging.dead-letter.protocol)

(defn normalize-error-info
  "Normalizes `error-info` to be safe for logging/encoding.

  Expected input shape:
  - `{:error <Exception|string|keyword|any> :stacktrace <string|nil> ...}`

  Normalization rules:
  - If `:error` is an Exception, it becomes a map with `:message`/`:class`.
  - If `:stacktrace` is non-string, it is coerced via `str`.
  - Unknown keys are preserved.

  Returns a map."
  [error-info]
  (let [err (:error error-info)
        err' (cond
               (instance? Throwable err) {:message (.getMessage ^Throwable err)
                                         :class (-> err class .getName)}
               :else err)
        st (:stacktrace error-info)
        st' (cond
              (nil? st) nil
              (string? st) st
              :else (str st))]
    (cond-> (assoc error-info :error err')
      (contains? error-info :stacktrace) (assoc :stacktrace st'))))

(defprotocol DeadLetterProtocol
  "Dead letter abstraction.

  A dead letter sink is responsible for capturing *failed* message processing so the
  message can be inspected/replayed later.

  ## Inputs
  - `envelope`: the original message envelope consumed by a handler.
    Conventionally includes `:metadata` (e.g. `{:trace ...}`) plus a message payload.
  - `error-info`: map describing the failure. At minimum:
    - `:error`: exception, string, keyword, or any useful error value
    - `:stacktrace`: stacktrace string (optional)
  - `opts`: runtime options that can tweak delivery behavior:
    - `:sink`: overrides the chosen sink when using the common facade
    - `:dlq-topic`: override destination topic (producer sink)
    - `:max-retries`: override max retries (producer sink)
    - `:delay-ms`: override delay before publishing (producer sink)
    - plus any backend-specific options (e.g. storage put opts)

  ## Return
  A result map that MUST include `:ok` boolean and SHOULD include `:sink` keyword.
  Implementations may add details like `:path`, `:topic`, `:retry-count`, etc."
  (send-dead-letter! [this envelope error-info opts]
    "Send a failed message to the configured dead letter sink."))

