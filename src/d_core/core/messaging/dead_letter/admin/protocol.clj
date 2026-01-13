(ns d-core.core.messaging.dead-letter.admin.protocol)

(defprotocol DeadLetterAdminProtocol
  "Transport-agnostic DLQ administration.

  Implementations are responsible for fetching and updating DLQ records using a
  durable backing store (storage is the default source of truth)."
  (list-deadletters [this query opts]
    "List DLQ records.

     query:
     - :topic (keyword|string, optional)
     - :status (keyword, optional)
     - :limit (int, optional)

     returns {:ok true :items [...]} or {:ok false :error ...}")
  (get-deadletter [this dlq-id opts]
    "Fetch a DLQ record by dlq-id.")
  (mark-deadletter! [this dlq-id status opts]
    "Update DLQ status (e.g. :manual/:stuck/:poison/:eligible).")
  (replay-deadletter! [this dlq-id opts]
    "Replay a DLQ record by dlq-id. Implementations should use generic producer APIs when possible."))

