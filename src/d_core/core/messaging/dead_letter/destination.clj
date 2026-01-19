(ns d-core.core.messaging.dead-letter.destination
  "Dead-letter destination derivation.

  This module provides transport-aware defaults so most topics do not need any
  explicit DLQ topic configuration.

  Convention (default): append `.dl` to the transport-specific destination name."
  (:require [d-core.core.messaging.routing :as routing]))

(def default-suffix ".dl")

(defn dlq-topic-key
  "Returns the derived DLQ topic keyword for an original topic keyword.

  Example: :orders -> :orders.dl"
  [topic]
  (keyword (str (name topic) default-suffix)))

(defn- suffix
  [envelope]
  (or (get-in envelope [:metadata :dlq :deadletter :suffix])
      default-suffix))

(defn dlq-destination
  "Returns a map describing how to publish to a topic's DLQ destination.

  Output keys:
  - `:topic` keyword (derived, e.g. :orders.dl)
  - `:producer` keyword (same as the original subscription producer/client key)
  - `:options` map of backend-specific overrides when the topic config uses explicit names
    (e.g. redis :stream, kafka :kafka-topic, jetstream :subject).

  Suffix can be overridden per-topic via routing `:deadletter {:suffix \".dl\"}`."
  [routing topic envelope]
  (let [runtime (get-in envelope [:metadata :dlq :runtime])
        producer-key (get-in envelope [:metadata :dlq :producer])
        cfg (routing/topic-config routing topic)
        suf (suffix envelope)
        dlq-topic (keyword (str (name topic) suf))
        opts (case runtime
               :redis (when-let [stream (:stream cfg)]
                        {:stream (str stream suf)})
               :redis-replay (when-let [stream (:stream cfg)]
                               {:stream (str stream suf)})
               :kafka (when-let [kt (:kafka-topic cfg)]
                        {:kafka-topic (str kt suf)})
               :jetstream (when-let [subj (:subject cfg)]
                            {:subject (str subj suf)})
               :in-memory nil
               nil)]
    {:topic dlq-topic
     :producer producer-key
     :options (or opts {})}))
