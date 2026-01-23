(ns d-core.core.messaging.routing
  (:require [integrant.core :as ig]
            [d-core.tracing :as tracing]))

(defn- deep-merge
  "Recursively merges maps. Non-map values on the right overwrite."
  [& xs]
  (letfn [(dm [a b]
            (if (and (map? a) (map? b))
              (merge-with dm a b)
              b))]
    (reduce dm {} xs)))

(defn- wrap-handler
  [handler-fn]
  (fn [envelope]
    (let [parent (some-> (get-in envelope [:metadata :trace]) tracing/decode-ctx)
          ctx (tracing/child-ctx parent)]
      (tracing/with-ctx ctx
        (handler-fn envelope)))))

(defn- resolve-subscriptions
  [subs handlers]
  (let [handlers (or handlers {})]
    (into {}
          (map (fn [[id sub]]
                 (let [h (:handler sub)]
                   (cond
                     (fn? h)
                     [id (update sub :handler wrap-handler)]

                     (and (keyword? h) (contains? handlers h))
                     [id (assoc sub :handler (wrap-handler (get handlers h)))]

                     :else
                     (throw (ex-info "Subscription handler must be a function or a known handler key"
                                     {:subscription id
                                      :handler h
                                      :known-handlers (keys handlers)}))))))
          (or subs {}))))

(defmethod ig/init-key :d-core.core.messaging/routing
  [_ routing]
  (let [routing (or routing {})]
    (cond
      (and (map? routing) (contains? routing :default-routing))
      (let [{:keys [default-routing overrides]} routing
            default-routing (or default-routing {})
            overrides (or overrides {})
            handlers (merge (:handlers default-routing) (:handlers overrides))
            merged (-> (deep-merge default-routing (dissoc overrides :handlers))
                       (assoc :handlers handlers))]
        (if (contains? merged :subscriptions)
          (update merged :subscriptions #(resolve-subscriptions % handlers))
          merged))

      (and (map? routing) (contains? routing :subscriptions))
      (update routing :subscriptions #(resolve-subscriptions % (:handlers routing)))

      :else
      routing)))

(defn topic-config
  [routing topic]
  (get-in routing [:topics topic]))

(defn publish-config
  [routing topic]
  (get-in routing [:publish topic]))

(defn publish-targets
  "Returns a vector of publish targets for `topic` (or empty when unset)."
  [routing topic]
  (let [targets (get-in routing [:publish topic :targets])]
    (cond
      (nil? targets) []
      (sequential? targets) (vec targets)
      :else [targets])))

(defn subscription-config
  [routing subscription-id]
  (get-in routing [:subscriptions subscription-id]))

;; Dead letter configuration
;;
;; The routing component can define dead letter configuration at three levels:
;; - `[:defaults :deadletter {...}]` (global defaults)
;; - `[:topics <topic> :deadletter {...}]` (per-topic overrides)
;; - `[:subscriptions <id> :deadletter {...}]` (per-subscription overrides)
;;
;; The dead-letter subsystem uses these settings to choose sink/policy/limits.
(defn default-deadletter
  [routing]
  (get-in routing [:defaults :deadletter] {}))

(defn deadletter-config
  "Returns the effective dead-letter configuration for `topic` by merging routing
  defaults with topic overrides and subscription overrides (when provided)."
  ([routing topic]
   (deadletter-config routing topic nil))
  ([routing topic subscription-id]
   (merge (default-deadletter routing)
          (get-in routing [:topics topic :deadletter] {})
          (when subscription-id
            (get-in routing [:subscriptions subscription-id :deadletter] {})))))
