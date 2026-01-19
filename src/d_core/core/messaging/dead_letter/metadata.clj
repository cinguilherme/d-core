(ns d-core.core.messaging.dead-letter.metadata
  "Helpers for enriching envelopes with standardized DLQ metadata.

  The canonical location for DLQ state is:
  - `[:metadata :dlq]`

  This metadata is designed to be portable across sinks (logger/storage/producer)
  and across replay mechanisms."
  (:import (java.util UUID Base64)
           (java.security MessageDigest)))

(defn- now-ms []
  (System/currentTimeMillis))

(defn raw-payload-meta
  "Returns a portable representation for a raw payload.

  - bytes  -> {:format :bytes-base64 :data <string>}
  - string -> {:format :string :data <string>}
  - nil    -> nil"
  [payload]
  (cond
    (nil? payload) nil
    (bytes? payload) {:format :bytes-base64
                      :data (.encodeToString (Base64/getEncoder) ^bytes payload)}
    (string? payload) {:format :string :data payload}
    :else {:format :unknown
           :data (pr-str payload)}))

(defn- bytes->hex
  ^String [^bytes bs]
  (let [sb (StringBuilder. (* 2 (alength bs)))]
    (dotimes [i (alength bs)]
      (.append sb (format "%02x" (bit-and 0xff (aget bs i)))))
    (.toString sb)))

(defn- sha256-hex
  ^String [^String s]
  (let [md (MessageDigest/getInstance "SHA-256")]
    (.update md (.getBytes s "UTF-8"))
    (bytes->hex (.digest md))))

(defn payload-hash
  "Computes a stable payload hash for DLQ identification/deduplication.

  Preference order:
  - raw payload (if available)
  - envelope :msg (fallback; stable and transport-agnostic)"
  [envelope raw-payload]
  (if raw-payload
    (sha256-hex (str (:format raw-payload) ":" (:data raw-payload)))
    (sha256-hex (pr-str (:msg envelope)))))

(defn ensure-envelope
  "Coerces `x` into an envelope-ish map if it isn't already one."
  [x]
  (if (map? x)
    x
    {:msg x
     :metadata {}}))

(defn enrich-for-deadletter
  "Adds/updates DLQ metadata on `envelope`.

  Inputs:
  - `ctx` may include:
    - `:topic` (keyword)
    - `:subscription-id` (keyword)
    - `:runtime` (keyword, e.g. :redis/:kafka/:jetstream/:in-memory)
    - `:source` (map, runtime-specific details like stream/offset/etc)
    - `:producer` (keyword, producer/client key to route DLQ publishes)
    - `:deadletter` (effective per-topic dl config; stored as a snapshot)
    - `:raw-payload` (string/bytes; stored as portable meta)
    - `:status` (default :eligible)

  Returns updated envelope map."
  [envelope {:keys [topic subscription-id runtime source deadletter raw-payload status producer]}]
  (let [envelope (ensure-envelope envelope)
        ts (now-ms)
        status (or status :eligible)
        raw (raw-payload-meta raw-payload)
        existing (get-in envelope [:metadata :dlq] {})
        ;; attempt is a replay concern; initialize if missing, never decrement.
        attempt (long (or (:attempt existing) 0))
        dlq-id (or (:id existing) (str (UUID/randomUUID)))
        first-failed-at (or (:first-failed-at existing) ts)
        ph (or (:payload-hash existing) (payload-hash envelope raw))]
    (cond-> envelope
      true (assoc-in [:metadata :dlq]
                     (cond-> (merge existing
                                    {:id dlq-id
                                     :payload-hash ph
                                     :status status
                                     :attempt attempt
                                     :first-failed-at first-failed-at
                                     :last-failed-at ts}
                                    (when topic {:topic topic})
                                    (when subscription-id {:subscription-id subscription-id})
                                    (when runtime {:runtime runtime})
                                    (when (map? source) {:source source})
                                    (when producer {:producer producer})
                                    (when (map? deadletter) {:deadletter deadletter}))
                       raw (assoc :raw-payload raw))))))
