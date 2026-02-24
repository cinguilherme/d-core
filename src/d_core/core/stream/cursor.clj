(ns d-core.core.stream.cursor
  (:require [d-core.core.pagination.token :as token]))

(defn encode-cursor
  [{:keys [conversation-id cursor direction source]}]
  (token/encode-token {:conversation_id (str conversation-id)
                       :cursor cursor
                       :direction (name direction)
                       :source (name source)}))

(defn decode-cursor
  [cursor-token]
  (let [decoded (token/decode-token cursor-token)
        source (some-> (:source decoded) keyword)
        direction (some-> (:direction decoded) keyword)
        cursor (:cursor decoded)
        seq-cursor (when (number? cursor) (long cursor))]
    {:token decoded
     :source source
     :direction direction
     :cursor cursor
     :seq-cursor seq-cursor
     :conversation-id (:conversation_id decoded)}))
