(ns d-core.libs.time
  (:require [integrant.core :as ig])
  (:import [java.time Clock Instant ZoneId Duration]
           [java.time.temporal ChronoUnit]))

(def ^:private unit->chrono
  {:nano ChronoUnit/NANOS
   :nanos ChronoUnit/NANOS
   :micro ChronoUnit/MICROS
   :micros ChronoUnit/MICROS
   :millis ChronoUnit/MILLIS
   :ms ChronoUnit/MILLIS
   :second ChronoUnit/SECONDS
   :seconds ChronoUnit/SECONDS
   :minute ChronoUnit/MINUTES
   :minutes ChronoUnit/MINUTES
   :hour ChronoUnit/HOURS
   :hours ChronoUnit/HOURS
   :half-day ChronoUnit/HALF_DAYS
   :half-days ChronoUnit/HALF_DAYS
   :day ChronoUnit/DAYS
   :days ChronoUnit/DAYS})

(defn ^:private chrono-unit [unit]
  (or (unit->chrono unit)
      (throw (ex-info "Unsupported time unit"
                      {:unit unit
                       :supported (sort (keys unit->chrono))}))))

(defn ^:private coerce-zone [zone]
  (cond
    (nil? zone) (ZoneId/systemDefault)
    (instance? ZoneId zone) zone
    (string? zone) (ZoneId/of zone)
    (keyword? zone) (ZoneId/of (name zone))
    :else (throw (ex-info "Unsupported zone value" {:zone zone}))))

(defn instant-map?
  "Returns true if the value looks like an instant map.

  Supported shapes:
  - {:epoch-ms <long>}
  - {:epoch-second <long> :nano <int>}"
  [value]
  (and (map? value)
       (or (contains? value :epoch-ms)
           (and (contains? value :epoch-second)
                (contains? value :nano)))))

(defn instant?
  "Returns true if the value is a java.time.Instant."
  [value]
  (instance? Instant value))

(defn ^:private coerce-instant [value]
  (cond
    (instance? Instant value) value
    (number? value) (Instant/ofEpochMilli (long value))
    (instance? java.util.Date value) (.toInstant ^java.util.Date value)
    (instant-map? value)
    (if (contains? value :epoch-ms)
      (Instant/ofEpochMilli (long (:epoch-ms value)))
      (Instant/ofEpochSecond (long (:epoch-second value))
                             (long (:nano value))))
    :else
    (throw (ex-info "Unsupported instant value" {:value value}))))

(defn instant->map
  "Converts an Instant into a time map.

  The map uses {:epoch-ms <long>} and optionally a :zone string."
  ([^Instant inst]
   (instant->map inst nil))
  ([^Instant inst zone]
   (cond-> {:epoch-ms (.toEpochMilli inst)}
     zone (assoc :zone (str zone)))))

(defn map->instant
  "Converts a time map into an Instant."
  [m]
  (coerce-instant m))

(defn duration
  "Creates a Duration. Accepts a map like {:seconds 1 :millis 250},
  a Duration instance, or an amount + unit."
  ([amount unit]
   (Duration/of (long amount) (chrono-unit unit)))
  ([value]
   (cond
     (instance? Duration value) value
     (map? value) (reduce-kv (fn [^Duration d k v]
                               (.plus d (duration v k)))
                             Duration/ZERO
                             value)
     :else (throw (ex-info "Unsupported duration value" {:value value})))))

(defn duration->map
  "Converts a Duration to a map with a :millis key."
  [^Duration d]
  {:millis (.toMillis d)})

(defn new-clock
  "Creates a java.time.Clock.

  Options:
  - {:type :system :zone \"UTC\"}
  - {:type :fixed :instant <instant|map|epoch-ms> :zone \"UTC\"}
  - {:type :offset :clock <clock|opts> :offset <duration|map>}
  - {:type :tick :clock <clock|opts> :tick <duration|map>}"
  ([] (Clock/systemDefaultZone))
  ([{:keys [type zone instant clock offset tick]
     :or {type :system}}]
   (let [zone-id (coerce-zone zone)
         base (cond
                (instance? Clock clock) clock
                (map? clock) (new-clock clock)
                (nil? clock) (Clock/system zone-id)
                :else (throw (ex-info "Unsupported clock value" {:clock clock})))]
     (case type
       :system (Clock/system zone-id)
       :fixed (Clock/fixed (coerce-instant instant) zone-id)
       :offset (Clock/offset base (duration offset))
       :tick (Clock/tick base (duration tick))
       (throw (ex-info "Unsupported clock type" {:type type}))))))

(defn now
  "Returns the current Instant for the provided Clock (or system default)."
  ([] (Instant/now))
  ([^Clock clock]
   (Instant/now clock)))

(defn now-map
  "Returns a time map for now, optionally including the clock zone."
  ([] (instant->map (now)))
  ([^Clock clock]
   (instant->map (now clock) (.getZone clock))))

(defn plus
  "Adds a duration to a Clock, Instant, or time map.

  When called with a Clock, uses now as the base.
  When called with a map, returns a map."
  ([value dur]
   (let [inst (if (instance? Clock value)
                (now ^Clock value)
                (coerce-instant value))
         result (.plus inst (duration dur))]
     (if (map? value)
       (instant->map result (:zone value))
       result)))
  ([value unit amount]
   (plus value (duration amount unit))))

(defn minus
  "Subtracts a duration from a Clock, Instant, or time map."
  ([value dur]
   (plus value (.negated (duration dur))))
  ([value unit amount]
   (plus value unit (- (long amount)))))

(defn since
  "Returns a Duration between an instant (or map) and now."
  ([instant]
   (Duration/between (coerce-instant instant) (now)))
  ([^Clock clock instant]
   (Duration/between (coerce-instant instant) (now clock))))

(defn compare-instants
  "Compares two instants (or maps/epoch values). Returns -1, 0, or 1."
  [a b]
  (let [result (compare (coerce-instant a) (coerce-instant b))]
    (cond
      (neg? result) -1
      (pos? result) 1
      :else 0)))

(defn before?
  "Returns true if instant a is before instant b."
  [a b]
  (.isBefore (coerce-instant a) (coerce-instant b)))

(defn after?
  "Returns true if instant a is after instant b."
  [a b]
  (.isAfter (coerce-instant a) (coerce-instant b)))

(defn instant=
  "Returns true if both instants represent the same moment."
  [a b]
  (.equals (coerce-instant a) (coerce-instant b)))

(defn between?
  "Returns true if instant is within [start, end] inclusive."
  [instant start end]
  (let [inst (coerce-instant instant)
        start-inst (coerce-instant start)
        end-inst (coerce-instant end)]
    (and (not (.isBefore inst start-inst))
         (not (.isAfter inst end-inst)))))

(defmethod ig/init-key :d-core.libs.time/clock
  [_ opts]
  (new-clock (merge {:type :system :zone "UTC"} (or opts {}))))
