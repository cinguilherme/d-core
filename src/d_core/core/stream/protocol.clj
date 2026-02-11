(ns d-core.core.stream.protocol)

;; Protocol for stream backends, allowing capabilities 
;; such as redis streams and alternatives with the same protocol.

(defprotocol StreamBackend
  (append-payload! [this stream payload-bytes])
  (append-batch! [this stream payloads-bytes])
  ;; opts requires :direction with one of #{:forward :backward}
  (read-payloads [this stream opts])
  (trim-stream! [this stream id])
  (list-streams [this pattern])
  (get-cursor [this key])
  (set-cursor! [this key cursor])
  (next-sequence! [this key]))
