(ns d-core.core.clients.tile38.client
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [taoensso.carmine :as car]
            [taoensso.carmine.commands :as car-commands]))

(defrecord Tile38Client [conn output]
  Object
  (toString [_]
    (str "#Tile38Client{:output " (pr-str output)
         ", :conn " (pr-str (dissoc conn :spec)) "}")))

(defn make-client
  [{:keys [uri host port password timeout-ms pool output]
    :or {host "localhost"
         port 9851
         output :json}}]
  (let [spec (cond
               uri {:uri uri}
               :else (cond-> {:host host :port port}
                       password (assoc :password password)
                       timeout-ms (assoc :timeout-ms timeout-ms)))]
    (->Tile38Client {:pool (or pool {}) :spec spec} output)))

(defn- json-like?
  [s]
  (let [s (str/trim s)]
    (or (str/starts-with? s "{") (str/starts-with? s "["))))

(defn- maybe-parse-json
  [resp]
  (if (string? resp)
    (let [s (str/trim resp)]
      (if (json-like? s)
        (try
          (json/parse-string s true)
          (catch Exception _e
            resp))
        resp))
    resp))

(defn- request-opts
  [client opts]
  (let [default-parse? (= :json (:output client))]
    {:parse-json? (if (contains? opts :parse-json?)
                    (:parse-json? opts)
                    default-parse?)}))

(defn- command-token
  [command]
  (str/upper-case
    (cond
      (keyword? command) (name command)
      (symbol? command) (name command)
      :else (str command))))

;; Tile38 speaks Redis protocol, so we enqueue raw commands via Carmine.
(defn- request!
  [client command args opts]
  (let [cmd (command-token command)
        req (into [cmd] args)
        resp (car/wcar (:conn client)
               (car-commands/enqueue-request 0 req))
        {:keys [parse-json?]} (request-opts client opts)]
    (if parse-json?
      (maybe-parse-json resp)
      resp)))

(defn- fields->args
  [fields]
  (when (seq fields)
    (reduce
      (fn [acc [k v]]
        (conj acc "FIELD" (if (keyword? k) (name k) (str k)) v))
      []
      (sort-by (comp str key) fields))))

(defn- point->latlon
  [point]
  (cond
    (and (map? point) (contains? point :lat) (contains? point :lon))
    [(:lat point) (:lon point)]

    (and (map? point) (contains? point :latitude) (contains? point :longitude))
    [(:latitude point) (:longitude point)]

    (sequential? point)
    (let [[lat lon] point]
      (when (and (some? lat) (some? lon))
        [lat lon]))

    :else nil))

(defn- bounds->coords
  [bounds]
  (cond
    (map? bounds)
    [(:minlat bounds) (:minlon bounds) (:maxlat bounds) (:maxlon bounds)]

    (sequential? bounds)
    (let [[minlat minlon maxlat maxlon] bounds]
      [minlat minlon maxlat maxlon])

    :else nil))

(defn- object->json
  [object]
  (cond
    (string? object) object
    (map? object) (json/generate-string object)
    :else (throw (ex-info "Tile38 object must be string or map" {:object object}))))

(defn- ensure-point
  [point]
  (let [[lat lon] (point->latlon point)]
    (when-not (and (some? lat) (some? lon))
      (throw (ex-info "Tile38 point must include lat/lon" {:point point})))
    [lat lon]))

(defn- ensure-bounds
  [bounds]
  (let [[minlat minlon maxlat maxlon] (bounds->coords bounds)]
    (when-not (and (some? minlat) (some? minlon) (some? maxlat) (some? maxlon))
      (throw (ex-info "Tile38 bounds must include minlat/minlon/maxlat/maxlon" {:bounds bounds})))
    [minlat minlon maxlat maxlon]))

(defn- set-options
  [{:keys [fields ex nx? xx?]}]
  (when (and nx? xx?)
    (throw (ex-info "Tile38 SET cannot use NX and XX together" {:nx? nx? :xx? xx?})))
  (concat
    (or (fields->args fields) [])
    (when (some? ex) ["EX" ex])
    (when nx? ["NX"])
    (when xx? ["XX"])))

(defn set-point!
  [client key id point opts]
  (let [[lat lon] (ensure-point point)
        args (vec
               (concat
                 [key id]
                 (set-options opts)
                 ["POINT" lat lon]))]
    (request! client "SET" args opts)))

(defn set-object!
  [client key id object opts]
  (let [obj (object->json object)
        args (vec
               (concat
                 [key id]
                 (set-options opts)
                 ["OBJECT" obj]))]
    (request! client "SET" args opts)))

(defn set-bounds!
  [client key id bounds opts]
  (let [[minlat minlon maxlat maxlon] (ensure-bounds bounds)
        args (vec
               (concat
                 [key id]
                 (set-options opts)
                 ["BOUNDS" minlat minlon maxlat maxlon]))]
    (request! client "SET" args opts)))

(defn get!
  [client key id {:keys [type with-fields? field] :as opts}]
  (let [type-token (case type
                     :point "POINT"
                     :bounds "BOUNDS"
                     :object "OBJECT"
                     :hash "HASH"
                     :id nil
                     nil)
        args (cond-> [key id]
               with-fields? (conj "WITHFIELDS")
               field (conj "FIELD" field)
               type-token (conj type-token))]
    (request! client "GET" args opts)))

(defn del!
  [client key id & more-ids]
  (request! client "DEL" (into [key id] more-ids) {}))

(defn drop!
  [client key opts]
  (request! client "DROP" [key] opts))

(defn scan!
  [client key {:keys [cursor match limit] :as opts}]
  (let [args (cond-> [key]
               (some? cursor) (conj "CURSOR" cursor)
               match (conj "MATCH" match)
               limit (conj "COUNT" limit))]
    (request! client "SCAN" args opts)))

(defn nearby!
  [client key point {:keys [radius cursor match limit] :as opts}]
  (let [[lat lon] (ensure-point point)
        args (cond-> [key]
               (some? cursor) (conj "CURSOR" cursor)
               match (conj "MATCH" match)
               limit (conj "LIMIT" limit)
               true (conj "POINT" lat lon)
               (some? radius) (conj "RADIUS" radius))]
    (request! client "NEARBY" args opts)))

(defn- shape-args
  [{:keys [bounds object point]}]
  (cond
    bounds (let [[minlat minlon maxlat maxlon] (ensure-bounds bounds)]
             ["BOUNDS" minlat minlon maxlat maxlon])
    object ["OBJECT" (object->json object)]
    point (let [[lat lon] (ensure-point point)]
            ["POINT" lat lon])
    :else (throw (ex-info "Tile38 query requires :bounds, :object, or :point"
                          {:bounds bounds :object object :point point}))))

(defn within!
  [client key shape {:keys [cursor match limit] :as opts}]
  (let [args (vec
               (concat
                 (cond-> [key]
                   (some? cursor) (conj "CURSOR" cursor)
                   match (conj "MATCH" match)
                   limit (conj "LIMIT" limit))
                 (shape-args shape)))]
    (request! client "WITHIN" args opts)))

(defn intersects!
  [client key shape {:keys [cursor match limit] :as opts}]
  (let [args (vec
               (concat
                 (cond-> [key]
                   (some? cursor) (conj "CURSOR" cursor)
                   match (conj "MATCH" match)
                   limit (conj "LIMIT" limit))
                 (shape-args shape)))]
    (request! client "INTERSECTS" args opts)))
