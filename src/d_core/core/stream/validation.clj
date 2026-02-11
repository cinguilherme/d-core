(ns d-core.core.stream.validation)

(def valid-directions [:forward :backward])
(def valid-directions-set (set valid-directions))

(defn ensure-direction!
  [opts]
  (let [opts'     (or opts {})
        direction (:direction opts')]
    (when-not (contains? valid-directions-set direction)
      (throw (ex-info "Stream read requires :direction to be explicitly :forward or :backward."
                      {:component  :d-core.core.stream/read-payloads
                       :dependency :direction
                       :expected   valid-directions
                       :actual     direction
                       :hint       "Pass :direction :forward or :direction :backward in read-payloads opts."})))
    opts'))
