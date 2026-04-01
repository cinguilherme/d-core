(ns d-core.core.routing.schema
  "Canonical schemas for D-core routing.

  The public contract is intentionally small:
  - point-to-point route requests
  - source/target matrix requests
  - normalized route and matrix envelopes"
  (:require [clojure.string :as str]
            [d-core.core.schema :as schema]))

(defn- non-blank-string?
  [value]
  (and (string? value)
       (not (str/blank? value))))

(defn- strip-nil-values
  [value]
  (cond
    (map? value)
    (into (empty value)
          (keep (fn [[k v]]
                  (let [v' (strip-nil-values v)]
                    (when-not (nil? v')
                      [k v']))))
          value)

    (vector? value)
    (->> value
         (map strip-nil-values)
         (remove nil?)
         vec)

    (sequential? value)
    (->> value
         (map strip-nil-values)
         (remove nil?)
         doall)

    :else value))

(def non-blank-string-schema
  [:and string?
   [:fn {:error/message "expected a non-blank string"}
    (fn [value] (non-blank-string? value))]])

(def profile-schema
  [:enum :driving :walking :cycling])

(def units-schema
  [:enum :kilometers :miles])

(def point-schema
  [:map
   [:lat number?]
   [:lon number?]])

(def route-request-schema
  [:map
   [:locations [:vector {:min 2} point-schema]]
   [:profile {:optional true} profile-schema]
   [:alternatives {:optional true} [:or boolean? pos-int?]]
   [:steps {:optional true} boolean?]
   [:overview {:optional true} [:enum :full :simplified :none]]
   [:include-geometry {:optional true} boolean?]
   [:language {:optional true} non-blank-string-schema]
   [:units {:optional true} units-schema]])

(def matrix-request-schema
  [:map
   [:sources [:vector {:min 1} point-schema]]
   [:targets [:vector {:min 1} point-schema]]
   [:profile {:optional true} profile-schema]
   [:annotations {:optional true} [:vector [:enum :duration :distance]]]
   [:language {:optional true} non-blank-string-schema]
   [:units {:optional true} units-schema]
   [:verbose {:optional true} boolean?]])

(def route-leg-schema
  [:map
   [:distance number?]
   [:duration number?]
   [:summary {:optional true} string?]])

(def route-geometry-schema
  [:map
   [:format keyword?]
   [:value [:or string? map? [:vector string?]]]])

(def route-provider-schema
  [:map
   [:name keyword?]
   [:route-id {:optional true} string?]
   [:raw {:optional true} map?]])

(def route-item-schema
  [:map
   [:distance number?]
   [:duration number?]
   [:geometry {:optional true} route-geometry-schema]
   [:legs {:optional true} [:vector route-leg-schema]]
   [:provider route-provider-schema]
   [:metadata {:optional true} map?]])

(def route-envelope-schema
  [:map
   [:routes [:vector route-item-schema]]
   [:provider keyword?]
   [:metadata {:optional true} map?]])

(def matrix-value-schema
  [:or number? nil?])

(def matrix-row-schema
  [:vector matrix-value-schema])

(def matrix-values-schema
  [:vector matrix-row-schema])

(def matrix-location-schema
  [:map
   [:location point-schema]
   [:name {:optional true} string?]
   [:provider {:optional true} map?]
   [:metadata {:optional true} map?]])

(def ^:private matrix-has-data?
  [:fn {:error/message "matrix envelope requires :durations and/or :distances"}
   (fn [value]
     (or (contains? value :durations)
         (contains? value :distances)))])

(def matrix-envelope-schema
  [:and
   [:map
    [:durations {:optional true} matrix-values-schema]
    [:distances {:optional true} matrix-values-schema]
    [:sources [:vector matrix-location-schema]]
    [:targets [:vector matrix-location-schema]]
    [:provider keyword?]
    [:metadata {:optional true} map?]]
   matrix-has-data?])

(defn- normalize-point
  [point]
  (strip-nil-values point))

(defn normalize-route-request
  [request]
  (let [request' (strip-nil-values request)]
    (cond-> request'
      true (update :locations (fn [locations]
                                (mapv normalize-point locations)))
      (nil? (:profile request')) (assoc :profile :driving)
      (nil? (:overview request')) (assoc :overview :simplified)
      (nil? (:include-geometry request')) (assoc :include-geometry true)
      (nil? (:units request')) (assoc :units :kilometers))))

(defn normalize-matrix-request
  [request]
  (let [request' (strip-nil-values request)]
    (let [annotations (when (seq (:annotations request'))
                        (->> (:annotations request')
                             distinct
                             vec))]
      (cond-> request'
        true (update :sources (fn [sources] (mapv normalize-point sources)))
        true (update :targets (fn [targets] (mapv normalize-point targets)))
        (nil? (:profile request')) (assoc :profile :driving)
        (nil? (:units request')) (assoc :units :kilometers)
        (nil? (:verbose request')) (assoc :verbose false)
        annotations (assoc :annotations annotations)
        (and (contains? request' :annotations) (nil? annotations)) (dissoc :annotations)))))

(defn- normalize-route-item
  [item]
  (let [item' (strip-nil-values item)]
    (cond-> item'
      (vector? (:legs item')) (update :legs vec))))

(defn normalize-route-envelope
  [envelope]
  (let [envelope' (strip-nil-values envelope)]
    (update envelope' :routes
            (fn [routes]
              (mapv normalize-route-item (or routes []))))))

(defn- normalize-matrix-values
  [values]
  (when (vector? values)
    (mapv (fn [row]
            (if (vector? row)
              (mapv identity row)
              []))
          values)))

(defn- normalize-matrix-location
  [location]
  (let [location' (strip-nil-values location)]
    (update location' :location normalize-point)))

(defn normalize-matrix-envelope
  [envelope]
  (let [durations (:durations envelope)
        distances (:distances envelope)
        envelope' (strip-nil-values (dissoc envelope :durations :distances))]
    (cond-> envelope'
      true (update :sources (fn [sources]
                              (mapv normalize-matrix-location (or sources []))))
      true (update :targets (fn [targets]
                              (mapv normalize-matrix-location (or targets []))))
      (contains? envelope :durations) (assoc :durations (normalize-matrix-values durations))
      (contains? envelope :distances) (assoc :distances (normalize-matrix-values distances)))))

(defn validate-route-request!
  "Validate and normalize a route request."
  ([value] (validate-route-request! value {}))
  ([value opts]
   (let [value' (normalize-route-request value)]
     (schema/validate! route-request-schema value'
                       (merge {:schema-id :routing/route-request
                               :strictness :strict}
                              opts)))))

(defn validate-matrix-request!
  "Validate and normalize a matrix request."
  ([value] (validate-matrix-request! value {}))
  ([value opts]
   (let [value' (normalize-matrix-request value)]
     (schema/validate! matrix-request-schema value'
                       (merge {:schema-id :routing/matrix-request
                               :strictness :strict}
                              opts)))))

(defn validate-route-envelope!
  "Validate and normalize a route result envelope."
  ([value] (validate-route-envelope! value {}))
  ([value opts]
   (let [value' (normalize-route-envelope value)]
     (schema/validate! route-envelope-schema value'
                       (merge {:schema-id :routing/route-result
                               :strictness :strict}
                              opts)))))

(defn validate-matrix-envelope!
  "Validate and normalize a matrix result envelope."
  ([value] (validate-matrix-envelope! value {}))
  ([value opts]
   (let [value' (normalize-matrix-envelope value)]
     (schema/validate! matrix-envelope-schema value'
                       (merge {:schema-id :routing/matrix-result
                               :strictness :strict}
                              opts)))))
