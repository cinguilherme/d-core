(ns d-core.core.stream.in-mem.in-mem
  (:require [d-core.core.stream.protocol :as p-streams]
            [clojure.string :as str]
            [integrant.core :as ig]
            [d-core.core.stream.common :as common]
            [d-core.core.stream.in-mem.logic :as logic]
            [d-core.core.stream.validation :as validation]))

(defrecord InMemoryStreamBackend [state]
  p-streams/StreamBackend
  (append-payload! [_ stream payload-bytes]
    (let [now (System/currentTimeMillis)
          new-state (swap! state logic/append-entries-state stream [payload-bytes] now)
          id (get-in new-state [:last-ids stream])]
      (logic/notify-waiters! state stream)
      id))

  (append-batch! [_ stream payloads-bytes]
    (let [now (System/currentTimeMillis)
          old-last-id (get-in @state [:last-ids stream])
          _ (swap! state logic/append-entries-state stream payloads-bytes now)]
      (logic/notify-waiters! state stream)
      (let [res (logic/query-entries (get-in @state [:streams stream]) 
                                    {:direction :forward :cursor old-last-id})]
        (map :id (:entries res)))))

  (read-payloads [_ stream opts]
    (logic/read-payloads-with-blocking state stream (validation/ensure-direction! opts)))

  (trim-stream! [_ stream id]
    (swap! state update-in [:streams stream]
           (fn [entries]
             (if entries
               (into (sorted-map) (subseq entries > id))
               (sorted-map)))))

  (list-streams [_ pattern]
    (let [regex (re-pattern (str "^" (str/replace pattern "*" ".*") "$"))]
      (filter #(re-find regex %) (keys (:streams @state)))))

  (get-cursor [_ key]
    (get-in @state [:cursors key]))

  (set-cursor! [_ key cursor]
    (swap! state assoc-in [:cursors key] cursor))

  (next-sequence! [_ key]
    (get-in (swap! state update-in [:sequences key] (fnil inc 0))
            [:sequences key])))

(defmethod ig/init-key :core-service.app.streams.in-memory/backend
  [_ {:keys [logger metrics]}]
  (let [backend (->InMemoryStreamBackend (atom {:streams {} :cursors {} :sequences {} :last-ids {} :waiters {}}))]
    (common/wrap-backend backend logger metrics)))
