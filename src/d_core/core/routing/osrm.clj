(ns d-core.core.routing.osrm
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.core.http.client :as http]
            [d-core.core.routing.protocol :as p]
            [d-core.core.routing.schema :as schema]
            [integrant.core :as ig]))

(defn- parse-number
  [value]
  (cond
    (number? value) (double value)
    (string? value) (try
                      (Double/parseDouble value)
                      (catch NumberFormatException _
                        (throw (ex-info "OSRM payload has invalid numeric value"
                                        {:value value}))))
    :else nil))

(defn- non-empty-map
  [value]
  (when (and (map? value) (seq value))
    value))

(defn- response->json
  [response]
  (let [body (:body response)]
    (cond
      (map? body) body
      (string? body) (if (str/blank? body)
                       nil
                       (json/parse-string body true))
      :else nil)))

(defn- response-code
  [body]
  (some-> (:code body) str))

(defn- response-error
  [message response body]
  (throw (ex-info message
                  {:status (:status response)
                   :response response
                   :body body})))

(defn- no-route-error?
  [body]
  (contains? #{"NoRoute" "NoSegment"} (response-code body)))

(defn- no-table-error?
  [body]
  (contains? #{"NoTable" "NoSegment"} (response-code body)))

(defn- osrm-profile
  [profile]
  (case profile
    :walking "walking"
    :cycling "cycling"
    :driving "driving"
    "driving"))

(defn- coordinates->path
  [locations]
  (str/join ";" (map (fn [{:keys [lat lon]}]
                       (str lon "," lat))
                     locations)))

(defn- overview->osrm
  [overview]
  (case overview
    :none "false"
    :full "full"
    :simplified "simplified"
    "simplified"))

(defn- geometry-format
  [geometry]
  (cond
    (string? geometry) :polyline6
    (map? geometry) :geojson
    (vector? geometry) :polyline6
    :else :unknown))

(defn- route-leg->result
  [leg]
  (cond-> {:distance (parse-number (:distance leg))
           :duration (parse-number (:duration leg))}
    (string? (:summary leg)) (assoc :summary (:summary leg))))

(defn- route->result
  [route]
  (let [metadata (non-empty-map
                  {:weight (parse-number (:weight route))
                   :weight-name (:weight_name route)})]
    (cond-> {:distance (parse-number (:distance route))
             :duration (parse-number (:duration route))
             :provider {:name :osrm
                        :raw route}}
      (contains? route :geometry)
      (assoc :geometry {:format (geometry-format (:geometry route))
                        :value (:geometry route)})

      (vector? (:legs route))
      (assoc :legs (mapv route-leg->result (:legs route)))

      metadata
      (assoc :metadata metadata))))

(defn- waypoint->metadata
  [waypoint]
  (let [location (:location waypoint)
        coords-ok? (and (vector? location)
                        (<= 2 (count location))
                        (every? number? location))]
    (cond-> {}
      (string? (:name waypoint)) (assoc :name (:name waypoint))
      (contains? waypoint :distance) (assoc :distance (parse-number (:distance waypoint)))
      coords-ok? (assoc :location {:lat (parse-number (second location))
                                   :lon (parse-number (first location))}))))

(defn- route-envelope
  [body]
  (let [waypoints (when (vector? (:waypoints body))
                    (->> (:waypoints body)
                         (map waypoint->metadata)
                         (filter seq)
                         vec))
        metadata (non-empty-map
                  {:code (response-code body)
                   :message (:message body)
                   :data-version (:data_version body)
                   :waypoints waypoints})]
    (schema/validate-route-envelope!
     (cond-> {:routes (mapv route->result (or (:routes body) []))
              :provider :osrm}
       metadata (assoc :metadata metadata)))))

(defn- annotations->osrm
  [annotations]
  (let [annotations (set (or annotations [:duration :distance]))]
    (cond
      (= annotations #{:duration}) "duration"
      (= annotations #{:distance}) "distance"
      :else "duration,distance")))

(defn- requested-annotations
  [request]
  (set (or (:annotations request) [:duration :distance])))

(defn- matrix-values
  [values]
  (mapv (fn [row]
          (mapv (fn [value]
                  (when-not (nil? value)
                    (parse-number value)))
                (or row [])))
        (or values [])))

(defn- matrix-location
  [waypoint fallback]
  (let [location (:location waypoint)
        waypoint-point (when (and (vector? location)
                                  (<= 2 (count location))
                                  (every? number? location))
                         {:lat (parse-number (second location))
                          :lon (parse-number (first location))})
        point (or waypoint-point fallback)]
    (cond-> {:location point}
      (string? (:name waypoint)) (assoc :name (:name waypoint))
      (contains? waypoint :distance) (assoc :metadata {:distance (parse-number (:distance waypoint))}))))

(defn- request-matrix-locations
  [locations]
  (mapv (fn [location]
          {:location {:lat (parse-number (:lat location))
                      :lon (parse-number (:lon location))}})
        locations))

(defn- matrix-envelope
  [request body]
  (let [annotations (requested-annotations request)
        sources-request (:sources request)
        targets-request (:targets request)
        sources-body (:sources body)
        targets-body (:destinations body)
        sources (if (vector? sources-body)
                  (mapv (fn [idx source]
                          (matrix-location source (nth sources-request idx nil)))
                        (range (count sources-body))
                        sources-body)
                  (request-matrix-locations sources-request))
        targets (if (vector? targets-body)
                  (mapv (fn [idx target]
                          (matrix-location target (nth targets-request idx nil)))
                        (range (count targets-body))
                        targets-body)
                  (request-matrix-locations targets-request))
        metadata (non-empty-map
                  {:code (response-code body)
                   :message (:message body)
                   :data-version (:data_version body)
                   :fallback-speed-cells (:fallback_speed_cells body)})]
    (schema/validate-matrix-envelope!
     (cond-> {:sources sources
              :targets targets
              :provider :osrm}
       (and (contains? annotations :duration) (contains? body :durations))
       (assoc :durations (matrix-values (:durations body)))

       (and (contains? annotations :distance) (contains? body :distances))
       (assoc :distances (matrix-values (:distances body)))

       metadata
       (assoc :metadata metadata)))))

(defn- nil-matrix
  [rows cols]
  (vec (repeat rows (vec (repeat cols nil)))))

(defn- empty-route-envelope
  [body]
  (let [metadata (non-empty-map
                  {:code (response-code body)
                   :message (:message body)})]
    (schema/validate-route-envelope!
     (cond-> {:routes []
              :provider :osrm}
       metadata (assoc :metadata metadata)))))

(defn- empty-matrix-envelope
  [request body]
  (let [annotations (requested-annotations request)
        source-count (count (:sources request))
        target-count (count (:targets request))
        metadata (non-empty-map
                  {:code (response-code body)
                   :message (:message body)})
        envelope (cond-> {:sources (request-matrix-locations (:sources request))
                          :targets (request-matrix-locations (:targets request))
                          :provider :osrm}
                   (contains? annotations :duration)
                   (assoc :durations (nil-matrix source-count target-count))

                   (contains? annotations :distance)
                   (assoc :distances (nil-matrix source-count target-count))

                   metadata
                   (assoc :metadata metadata))]
    (schema/validate-matrix-envelope! envelope)))

(defn- route-query-params
  [request]
  (let [params {:overview (overview->osrm (:overview request))}]
    (cond-> params
      (:include-geometry request) (assoc :geometries "polyline6")
      (not (:include-geometry request)) (assoc :overview "false")
      (contains? request :steps) (assoc :steps (:steps request))
      (contains? request :alternatives) (assoc :alternatives (:alternatives request)))))

(defn- matrix-query-params
  [request]
  (let [source-count (count (:sources request))
        target-count (count (:targets request))
        coordinates (vec (concat (:sources request) (:targets request)))
        source-indices (range source-count)
        target-indices (range source-count (+ source-count target-count))]
    {:coordinates coordinates
     :query-params {:sources (str/join ";" source-indices)
                    :destinations (str/join ";" target-indices)
                    :annotations (annotations->osrm (:annotations request))}}))

(defn- request!
  [{:keys [http-client user-agent]} path query-params opts]
  (let [request {:method :get
                 :path path
                 :as :text
                 :headers {"Accept" "application/json"
                           "User-Agent" user-agent}
                 :query-params query-params}]
    (http/request! http-client
                   (cond-> request
                     (:request opts) (merge (:request opts))))))

(defrecord OsrmRouter [http-client user-agent]
  p/RoutingProtocol
  (route [this request opts]
    (let [request (schema/validate-route-request! request)
          profile (osrm-profile (:profile request))
          path (str "/route/v1/" profile "/" (coordinates->path (:locations request)))
          response (request! this path (route-query-params request) opts)
          body (response->json response)]
      (cond
        (and (<= 200 (:status response) 299)
             (or (= "Ok" (response-code body))
                 (vector? (:routes body))))
        (route-envelope (or body {}))

        (or (no-route-error? body)
            (and (<= 200 (:status response) 299)
                 (empty? (:routes body))))
        (empty-route-envelope body)

        :else
        (response-error "OSRM route request failed" response body))))

  (matrix [this request opts]
    (let [request (schema/validate-matrix-request! request)
          profile (osrm-profile (:profile request))
          {:keys [coordinates query-params]} (matrix-query-params request)
          path (str "/table/v1/" profile "/" (coordinates->path coordinates))
          response (request! this path query-params opts)
          body (response->json response)]
      (cond
        (and (<= 200 (:status response) 299)
             (= "Ok" (response-code body)))
        (matrix-envelope request (or body {}))

        (no-table-error? body)
        (empty-matrix-envelope request body)

        :else
        (response-error "OSRM matrix request failed" response body)))))

(defmethod ig/init-key :d-core.core.routing.osrm/router
  [_ {:keys [http-client user-agent] :as opts}]
  (when-not http-client
    (throw (ex-info "OSRM router requires :http-client" {:opts opts})))
  (->OsrmRouter http-client
                (or user-agent "d-core routing/osrm")))
