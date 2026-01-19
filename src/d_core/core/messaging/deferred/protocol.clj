(ns d-core.core.messaging.deferred.protocol)

(defprotocol DeferredScheduler
  "Schedules messages for future delivery.

  Implementations should accept a payload map:
  {:producer <producer-key>
   :msg <message>
   :options <producer-options>
   :deliver-at-ms <epoch-ms>}"
  (schedule! [scheduler payload]
    "Schedule a message for deferred delivery."))
