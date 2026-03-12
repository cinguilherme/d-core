(ns d-core.core.geofence.schema
  "Canonical schemas for D-core geofencing.

  These schemas define the public contract that applications and backend
  implementations use to exchange:
  - tracked objects written to the geo index
  - fence definitions managed by the geofence runtime
  - normalized events emitted from backend hook payloads

  They intentionally stop at spatial/proximity concerns. Match scoring,
  dispatch assignment, dating preferences, and other business logic stay
  app-owned."
  (:require [d-core.core.schema :as schema]))

(def id-schema
  [:or string? keyword?])

(def key-schema
  [:or string? keyword?])

(def point-schema
  [:map
   [:lat number?]
   [:lon number?]])

(def point-geometry-schema
  [:map
   [:kind [:= :point]]
   [:point point-schema]])

(def bounds-shape-schema
  [:map
   [:kind [:= :bounds]]
   [:minlat number?]
   [:minlon number?]
   [:maxlat number?]
   [:maxlon number?]])

(def circle-shape-schema
  [:map
   [:kind [:= :circle]]
   [:center point-schema]
   [:radius-m pos-int?]])

(def geojson-shape-schema
  [:map
   [:kind [:= :geojson]]
   [:geometry [:or map? string?]]])

(def shape-ref-schema
  [:map
   [:kind [:= :shape-ref]]
   [:key key-schema]
   [:id id-schema]])

(def shape-schema
  [:multi {:dispatch :kind}
   [:bounds bounds-shape-schema]
   [:circle circle-shape-schema]
   [:geojson geojson-shape-schema]
   [:shape-ref shape-ref-schema]])

(def tracked-geometry-schema
  [:multi {:dispatch :kind}
   [:point point-geometry-schema]
   [:bounds bounds-shape-schema]
   [:geojson geojson-shape-schema]])

(def filter-schema
  [:multi {:dispatch :kind}
   [:eq
    [:map
     [:kind [:= :eq]]
     [:field string?]
     [:value some?]]]
   [:range
    [:map
     [:kind [:= :range]]
     [:field string?]
     [:min number?]
     [:max number?]]]
   [:in
    [:map
     [:kind [:= :in]]
     [:field string?]
     [:values [:vector some?]]]]
   [:expr
    [:map
     [:kind [:= :expr]]
     [:expression string?]]]])

(def source-schema
  [:map
   [:key key-schema]
   [:match {:optional true} string?]
   [:filters {:optional true} [:vector filter-schema]]])

(def delivery-schema
  [:map
   [:endpoint string?]
   [:output {:optional true} [:enum :json :resp]]])

(def static-fence-schema
  [:map
   [:id id-schema]
   [:kind [:= :static]]
   [:source source-schema]
   [:shape shape-schema]
   [:operation [:enum :nearby :within :intersects]]
   [:delivery delivery-schema]
   [:detect {:optional true} [:set [:enum :inside :outside :enter :exit :cross]]]
   [:commands {:optional true} [:set [:enum :set :del :drop]]]
   [:metadata {:optional true} map?]])

(def roam-target-schema
  [:map
   [:key key-schema]
   [:pattern string?]
   [:radius-m pos-int?]])

(def roam-fence-schema
  [:map
   [:id id-schema]
   [:kind [:= :roam]]
   [:source source-schema]
   [:target roam-target-schema]
   [:delivery delivery-schema]
   [:detect {:optional true} [:set [:= :roam]]]
   [:commands {:optional true} [:set [:enum :set :del]]]
   [:no-dwell? {:optional true} boolean?]
   [:metadata {:optional true} map?]])

(def fence-schema
  [:multi {:dispatch :kind}
   [:static static-fence-schema]
   [:roam roam-fence-schema]])

(def tracked-object-schema
  [:map
   [:key key-schema]
   [:id id-schema]
   [:geometry tracked-geometry-schema]
   [:fields {:optional true} map?]
   [:ttl-seconds {:optional true} pos-int?]
   [:metadata {:optional true} map?]])

(def event-object-schema
  [:map
   [:key key-schema]
   [:id id-schema]
   [:geometry {:optional true} [:or point-schema map? string?]]
   [:fields {:optional true} map?]
   [:distance-m {:optional true} number?]])

(def provider-schema
  [:map
   [:hook {:optional true} string?]
   [:group {:optional true} string?]
   [:time {:optional true} [:or int? string?]]
   [:raw {:optional true} map?]])

(def event-schema
  [:map
   [:backend [:= :tile38]]
   [:fence-id id-schema]
   [:fence-kind [:enum :static :roam]]
   [:detect [:enum :inside :outside :enter :exit :cross :roam]]
   [:command {:optional true} [:enum :set :del :drop]]
   [:subject event-object-schema]
   [:counterparty {:optional true} event-object-schema]
   [:provider {:optional true} provider-schema]
   [:metadata {:optional true} map?]])

(defn validate-tracked-object!
  "Validate a tracked object written to the geo index."
  ([value] (validate-tracked-object! value {}))
  ([value opts]
   (schema/validate! tracked-object-schema value
                     (merge {:schema-id :geofence/tracked-object
                             :strictness :strict}
                            opts))))

(defn validate-fence!
  "Validate a fence definition before backend compilation."
  ([value] (validate-fence! value {}))
  ([value opts]
   (schema/validate! fence-schema value
                     (merge {:schema-id :geofence/fence
                             :strictness :strict}
                            opts))))

(defn validate-event!
  "Validate a normalized geofence event before publishing or handling."
  ([value] (validate-event! value {}))
  ([value opts]
   (schema/validate! event-schema value
                     (merge {:schema-id :geofence/event
                             :strictness :strict}
                            opts))))
