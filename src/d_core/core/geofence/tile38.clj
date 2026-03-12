(ns d-core.core.geofence.tile38
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.core.clients.tile38.client :as tc]
            [d-core.core.geofence.protocol :as p]
            [d-core.core.geofence.schema :as schema]
            [integrant.core :as ig]))

(defn- name-str
  [x]
  (cond
    (keyword? x) (name x)
    (symbol? x) (name x)
    :else (str x)))

(defn- maybe-json-map
  [x]
  (cond
    (map? x) x
    (string? x) (try
                  (json/parse-string x true)
                  (catch Exception _e
                    x))
    :else x))

(defn- object->json
  [object]
  (cond
    (string? object) object
    (map? object) (json/generate-string object)
    :else (throw (ex-info "GeoJSON object must be string or map" {:object object}))))

(defn- detect-args
  [detect]
  (when (seq detect)
    ["DETECT" (str/join "," (sort (map name-str detect)))]))

(defn- command-args
  [commands]
  (when (seq commands)
    ["COMMANDS" (str/join "," (sort (map name-str commands)))]))

(defn- filter->args
  [{:keys [kind field value values min max expression] :as filter-spec}]
  (case kind
    :eq ["WHEREIN" field 1 value]
    :in (into ["WHEREIN" field (count values)] values)
    :range ["WHERE" field min max]
    :expr (throw (ex-info "Tile38 :expr filters are not compiled yet; validate syntax against WHEREEVAL/expressions first"
                          {:filter filter-spec
                           :unsupported :expr}))
    (throw (ex-info "Unknown geofence filter kind" {:filter filter-spec}))))

(defn- source-args
  [{:keys [match filters]}]
  (vec
   (concat
    (when match ["MATCH" match])
    (mapcat filter->args filters))))

(defn- bounds-args
  [{:keys [minlat minlon maxlat maxlon]}]
  ["BOUNDS" minlat minlon maxlat maxlon])

(defn- point-args
  [{:keys [lat lon]}]
  ["POINT" lat lon])

(defn- compile-static-shape
  [{:keys [operation shape] :as fence}]
  (case operation
    :nearby
    (let [{:keys [kind center radius-m]} shape]
      (when-not (= :circle kind)
        (throw (ex-info "Tile38 :nearby fences currently require a :circle shape"
                        {:fence fence
                         :shape shape})))
      (into (point-args center) [radius-m]))

    :within
    (case (:kind shape)
      :bounds (bounds-args shape)
      :geojson ["OBJECT" (object->json (:geometry shape))]
      :shape-ref ["GET" (name-str (:key shape)) (name-str (:id shape))]
      (throw (ex-info "Tile38 :within fences require :bounds, :geojson, or :shape-ref"
                      {:fence fence
                       :shape shape})))

    :intersects
    (case (:kind shape)
      :bounds (bounds-args shape)
      :geojson ["OBJECT" (object->json (:geometry shape))]
      :shape-ref ["GET" (name-str (:key shape)) (name-str (:id shape))]
      (throw (ex-info "Tile38 :intersects fences require :bounds, :geojson, or :shape-ref"
                      {:fence fence
                       :shape shape})))

    (throw (ex-info "Unknown static fence operation" {:fence fence
                                                      :operation operation}))))

(defn- operation-token
  [operation]
  (case operation
    :nearby "NEARBY"
    :within "WITHIN"
    :intersects "INTERSECTS"
    (throw (ex-info "Unknown Tile38 operation" {:operation operation}))))

(defn- compile-static-fence-args
  [{:keys [source operation detect commands shape] :as fence}]
  (vec
   (concat
    [(operation-token operation) (name-str (:key source))]
    (source-args source)
    ["FENCE"]
    (detect-args detect)
    (command-args commands)
    (compile-static-shape (assoc fence :shape shape)))))

(defn- compile-roam-fence-args
  [{:keys [source target commands no-dwell?]}]
  (vec
   (concat
    ["NEARBY" (name-str (:key source))]
    (source-args source)
    ["FENCE"]
    (command-args commands)
    (when no-dwell? ["NODWELL"])
    ["ROAM" (name-str (:key target)) (:pattern target) (:radius-m target)])))

(defn compile-fence-args
  [fence]
  (let [fence (schema/validate-fence! fence)]
    (case (:kind fence)
      :static (compile-static-fence-args fence)
      :roam (compile-roam-fence-args fence)
      (throw (ex-info "Unknown geofence kind" {:fence fence})))))

(defn- compile-test-args
  [{:keys [kind operation] :as fence}]
  (when-not (= :static kind)
    (throw (ex-info "Tile38 TEST only supports static fence validation"
                    {:fence fence
                     :kind kind})))
  (when (= :nearby operation)
    (throw (ex-info "Tile38 TEST is not used for :nearby fences; compile-only validation is supported instead"
                    {:fence fence
                     :operation operation})))
  (into [(operation-token operation)] (compile-static-shape fence)))

(defn- hook-items
  [resp]
  (cond
    (map? resp) (or (:hooks resp) (:items resp) [])
    (sequential? resp) (vec resp)
    :else []))

(defn- hook-name
  [item]
  (or (:hook item)
      (:name item)
      (:id item)))

(defn- normalize-detect
  [x]
  (some-> x name-str str/lower-case keyword))

(defn- normalize-command
  [x]
  (some-> x name-str str/lower-case keyword))

(defn- geojson-point->point
  [object]
  (let [object (maybe-json-map object)]
    (if (and (map? object)
             (= "Point" (:type object))
             (vector? (:coordinates object))
             (<= 2 (count (:coordinates object))))
      (let [[lon lat] (:coordinates object)]
        {:lat lat :lon lon})
      object)))

(defn- normalize-geometry
  [m]
  (cond
    (contains? m :point)
    (let [point (:point m)]
      (cond
        (map? point) point
        (and (sequential? point) (<= 2 (count point)))
        (let [[lat lon] point]
          {:lat lat :lon lon})
        :else point))

    (contains? m :object)
    (geojson-point->point (:object m))

    :else nil))

(defn- distance-value
  [m]
  (or (:meters m)
      (:distance m)
      (:distance-m m)
      (:distance_m m)
      (:distance-meters m)
      (:distance_meters m)))

(defn- normalize-event-object
  [m opts]
  (when (map? m)
    (cond-> {:key (or (:key m)
                      (:collection m)
                      (:default-key opts))
             :id (or (:id m) (:name m))}
      (normalize-geometry m) (assoc :geometry (normalize-geometry m))
      (:fields m) (assoc :fields (:fields m))
      (some? (distance-value m)) (assoc :distance-m (distance-value m)))))

(defrecord Tile38Geofence [client]
  p/GeofenceProtocol
  (upsert-fence! [_ fence opts]
    (let [fence (schema/validate-fence! fence)
          args (compile-fence-args fence)]
      (tc/sethook! client (:id fence) (get-in fence [:delivery :endpoint]) args opts)))

  (get-fence [_ fence-id opts]
    (let [resp (tc/hooks! client (merge {:pattern (name-str fence-id)
                                         :limit 100}
                                        opts))
          item (some #(when (= (name-str fence-id) (hook-name %)) %) (hook-items resp))]
      (cond
        (and (map? resp) (false? (get resp :ok true)))
        {:ok false
         :error (or (:err resp) :backend-error)
         :raw resp
         :fence-id fence-id}

        item
        {:ok true :item item :raw resp}

        :else
        {:ok false :error :not-found :raw resp :fence-id fence-id})))

  (list-fences [_ query opts]
    (let [resp (tc/hooks! client (merge query opts))]
      {:ok (if (map? resp) (get resp :ok true) true)
       :items (hook-items resp)
       :cursor (or (:cursor resp) 0)
       :raw resp}))

  (delete-fence! [_ fence-id opts]
    (tc/delhook! client fence-id opts))

  (test-fence [_ fence opts]
    (let [fence (schema/validate-fence! fence)
          compiled (compile-fence-args fence)]
      (if (and (:key opts) (:id opts) (= :static (:kind fence)) (not= :nearby (:operation fence)))
        (let [test-args (compile-test-args fence)]
          {:ok true
           :compiled compiled
           :result (tc/test! client (:key opts) (:id opts) test-args opts)})
        {:ok true
         :mode :compile-only
         :compiled compiled})))

  (normalize-event [_ payload opts]
    (let [payload (maybe-json-map payload)
          counterparty (normalize-event-object (:nearby payload) {:default-key (:key payload)})
          subject (normalize-event-object payload {})
          detect (normalize-detect (:detect payload))
          command (normalize-command (:command payload))
          event (cond-> {:backend :tile38
                         :fence-id (or (:fence-id opts) (:hook payload))
                         :fence-kind (or (:fence-kind opts)
                                         (if (or (= :roam detect) counterparty) :roam :static))
                         :detect detect
                         :subject subject}
                  command (assoc :command command)
                  counterparty (assoc :counterparty counterparty)
                  true (assoc :provider (cond-> {:raw payload}
                                          (:hook payload) (assoc :hook (:hook payload))
                                          (:group payload) (assoc :group (:group payload))
                                          (:time payload) (assoc :time (:time payload))))
                  (:metadata opts) (assoc :metadata (:metadata opts)))]
      (if (false? (:validate? opts))
        event
        (schema/validate-event! event)))))

(defmethod ig/init-key :d-core.core.geofence.tile38/geofence
  [_ {:keys [tile38-client] :as opts}]
  (when-not tile38-client
    (throw (ex-info "Tile38 geofence runtime requires :tile38-client" {:opts opts})))
  (->Tile38Geofence tile38-client))
