(ns d-core.dev.dev
  (:require [d-core.core.clients.sqs.client :as sqs])
  (:import (java.util UUID)))

(defn sqs-smoke!
  "Create a queue, send/receive/delete a message, then delete the queue.
  Options map is passed to sqs/make-client; supports :endpoint/:region/:access-key/:secret-key.
  Returns a summary map with the message and queue details."
  ([] (sqs-smoke! {}))
  ([opts]
   (let [client (sqs/make-client opts)
         queue (str "dcore.dev.sqs." (UUID/randomUUID))
         queue-url (sqs/create-queue! client queue {})
         body (str "hello-" (UUID/randomUUID))]
     (try
       (sqs/send-message! client queue-url body {})
       (let [messages (sqs/receive-messages! client queue-url {:max-messages 1 :wait-seconds 1})
             msg (first messages)]
         (when msg
           (sqs/delete-message! client queue-url (:receipt-handle msg)))
         {:queue queue
          :queue-url queue-url
          :sent body
          :received (:body msg)})
       (finally
         (try
           (sqs/delete-queue! client queue-url)
           (catch Exception _e
             nil))))))
