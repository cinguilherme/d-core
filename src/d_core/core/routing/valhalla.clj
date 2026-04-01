(ns d-core.core.routing.valhalla
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
                        (throw (ex-info "Valhalla payload has invalid numeric value"
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

(defn- response-error
  [message response body]
  (throw (ex-info message
                  {:status (:status response)
                   :response response
                   :body body})))

(defn- costing
  [profile]
  (case profile
    :walking "pedestrian"
    :cycling "bicycle"
    :driving "auto"
    "auto"))

(defn- units-string
  [units]
  (if (= units :miles) "miles" "kilometers"))

(defn- distance->meters
  [distance units]
  (when-let [distance (parse-number distance)]
    (if (= units :miles)
      (* distance 1609.344)
      (* distance 1000.0))))

(defn- route-error-message
  [body]
  (or (:error body)
      (get-in body [:trip :status_message])
      (:status_message body)
      (:message body)
      ""))

(defn- no-route-error?
  [body]
  (let [trip-status (get-in body [:trip :status])
        error-code (:error_code body)
        message (route-error-message body)]
    (or (= 442 trip-status)
        (= 171 trip-status)
        (= 442 error-code)
        (= 171 error-code)
        (boolean (re-find #"(?i)(no path|no route|could not find route|unreachable)"
                          (str message))))))

(defn- no-matrix-error?
  [body]
  (let [error-code (:error_code body)
        message (or (:error body) (:message body) "")]
    (or (= 442 error-code)
        (= 171 error-code)
        (boolean (re-find #"(?i)(no path|no route|matrix|unreachable|no suitable edges)"
                          (str message))))))

(defn- matrix-request-locations
  [locations]
  (mapv (fn [{:keys [lat lon]}]
          {:location {:lat (parse-number lat)
                      :lon (parse-number lon)}})
        locations))

(defn- nil-matrix
  [rows cols]
  (vec (repeat rows (vec (repeat cols nil)))))

(defn- matrix-values
  [values]
  (mapv (fn [row]
          (mapv (fn [value]
                  (when-not (nil? value)
                    (parse-number value)))
                (or row [])))
        (or values [])))

(defn- matrix-distance-values
  [values units]
  (mapv (fn [row]
          (mapv (fn [value]
                  (when-not (nil? value)
                    (distance->meters value units)))
                (or row [])))
        (or values [])))

(defn- location-point
  [value fallback]
  (or (when (and (map? value)
                 (contains? value :lat)
                 (contains? value :lon))
        {:lat (parse-number (:lat value))
         :lon (parse-number (:lon value))})
      (when (and (map? value)
                 (vector? (:location value))
                 (<= 2 (count (:location value)))
                 (every? number? (:location value)))
        {:lat (parse-number (second (:location value)))
         :lon (parse-number (first (:location value)))})
      fallback))

(defn- matrix-location
  [value fallback]
  (let [point (location-point value fallback)]
    (cond-> {:location point}
      (string? (:name value)) (assoc :name (:name value))
      (contains? value :distance) (assoc :metadata {:distance (parse-number (:distance value))}))))

(defn- annotations-set
  [request]
  (set (or (:annotations request) [:duration :distance])))

(defn- route-request-body
  [request]
  (let [alternatives (:alternatives request)
        alternates (cond
                     (integer? alternatives) (max 0 alternatives)
                     (= alternatives true) 1
                     :else nil)]
    (cond-> {:locations (mapv (fn [{:keys [lat lon]}]
                                {:lat lat :lon lon})
                              (:locations request))
             :costing (costing (:profile request))
             :units (units-string (:units request))
             :shape_format (if (:include-geometry request)
                             "polyline6"
                             "no_shape")}
      (contains? request :steps) (assoc :narrative (:steps request))
      alternates (assoc :alternates alternates)
      (:language request) (assoc :directions_options {:language (:language request)}))))

(defn- matrix-request-body
  [request]
  (cond-> {:sources (mapv (fn [{:keys [lat lon]}]
                            {:lat lat :lon lon})
                          (:sources request))
           :targets (mapv (fn [{:keys [lat lon]}]
                            {:lat lat :lon lon})
                          (:targets request))
           :costing (costing (:profile request))
           :units (units-string (:units request))
           :verbose (:verbose request)}
    (:language request) (assoc :directions_options {:language (:language request)})))

(defn- request!
  [{:keys [http-client user-agent]} path payload opts]
  (let [request {:method :post
                 :path path
                 :as :text
                 :headers {"Accept" "application/json"
                           "Content-Type" "application/json"
                           "User-Agent" user-agent}
                 :body (json/generate-string payload)}]
    (http/request! http-client
                   (cond-> request
                     (:request opts) (merge (:request opts))))))

(defn- route-leg->result
  [leg units]
  (let [summary (:summary leg)]
    {:distance (distance->meters (:length summary) units)
     :duration (parse-number (:time summary))}))

(defn- route-item
  [trip units]
  (let [summary (:summary trip)
        legs (:legs trip)
        leg-shapes (->> legs
                        (map :shape)
                        (filter string?)
                        vec)
        geometry (cond
                   (= 1 (count leg-shapes))
                   {:format :polyline6
                    :value (first leg-shapes)}

                   (<= 2 (count leg-shapes))
                   {:format :polyline6
                    :value leg-shapes}

                   :else nil)
        metadata (non-empty-map
                  {:status (:status trip)
                   :status-message (:status_message trip)})]
    (cond-> {:distance (distance->meters (:length summary) units)
             :duration (parse-number (:time summary))
             :provider {:name :valhalla
                        :raw trip}}
      geometry (assoc :geometry geometry)
      (vector? legs) (assoc :legs (mapv #(route-leg->result % units) legs))
      metadata (assoc :metadata metadata))))

(defn- route-envelope
  [request body]
  (let [units (:units request)
        trip (:trip body)
        routes (if (map? trip)
                 [(route-item trip units)]
                 [])
        metadata (non-empty-map
                  {:id (:id body)
                   :units (units-string units)
                   :code (:status body)
                   :message (route-error-message body)})]
    (schema/validate-route-envelope!
     (cond-> {:routes routes
              :provider :valhalla}
       metadata (assoc :metadata metadata)))))

(defn- matrix-from-cells
  [rows cols cells value-key transform]
  (reduce (fn [matrix cell]
            (let [row (or (:from_index cell) (:source_index cell))
                  col (or (:to_index cell) (:target_index cell))
                  value (get cell value-key)]
              (if (and (integer? row)
                       (integer? col)
                       (< -1 row rows)
                       (< -1 col cols))
                (assoc-in matrix [row col] (when-not (nil? value)
                                             (transform value)))
                matrix)))
          (nil-matrix rows cols)
          cells))

(defn- matrix-envelope
  [request body]
  (let [units (:units request)
        rows (count (:sources request))
        cols (count (:targets request))
        annotations (annotations-set request)
        matrix-payload (if (map? (:sources_to_targets body))
                         (:sources_to_targets body)
                         body)
        sources-body (:sources body)
        targets-body (:targets body)
        cells (when (vector? (:sources_to_targets body))
                (:sources_to_targets body))
        durations (cond
                    (vector? (:durations matrix-payload)) (matrix-values (:durations matrix-payload))
                    (vector? cells) (matrix-from-cells rows cols cells :time parse-number)
                    :else nil)
        distances (cond
                    (vector? (:distances matrix-payload)) (matrix-distance-values (:distances matrix-payload) units)
                    (vector? cells) (matrix-from-cells rows cols cells :distance #(distance->meters % units))
                    :else nil)
        sources (if (vector? sources-body)
                  (mapv (fn [idx source]
                          (matrix-location source (nth (:sources request) idx nil)))
                        (range (count sources-body))
                        sources-body)
                  (matrix-request-locations (:sources request)))
        targets (if (vector? targets-body)
                  (mapv (fn [idx target]
                          (matrix-location target (nth (:targets request) idx nil)))
                        (range (count targets-body))
                        targets-body)
                  (matrix-request-locations (:targets request)))
        metadata (non-empty-map
                  {:units (units-string units)
                   :id (:id body)
                   :code (:status body)
                   :message (or (:error body) (:message body))})]
    (schema/validate-matrix-envelope!
     (cond-> {:sources sources
              :targets targets
              :provider :valhalla}
       (and (contains? annotations :duration) durations)
       (assoc :durations durations)

       (and (contains? annotations :distance) distances)
       (assoc :distances distances)

       metadata
       (assoc :metadata metadata)))))

(defn- empty-route-envelope
  [body]
  (let [metadata (non-empty-map
                  {:code (:error_code body)
                   :message (route-error-message body)})]
    (schema/validate-route-envelope!
     (cond-> {:routes []
              :provider :valhalla}
       metadata (assoc :metadata metadata)))))

(defn- empty-matrix-envelope
  [request body]
  (let [annotations (annotations-set request)
        source-count (count (:sources request))
        target-count (count (:targets request))
        metadata (non-empty-map
                  {:code (:error_code body)
                   :message (or (:error body) (:message body))})
        envelope (cond-> {:sources (matrix-request-locations (:sources request))
                          :targets (matrix-request-locations (:targets request))
                          :provider :valhalla}
                   (contains? annotations :duration)
                   (assoc :durations (nil-matrix source-count target-count))

                   (contains? annotations :distance)
                   (assoc :distances (nil-matrix source-count target-count))

                   metadata
                   (assoc :metadata metadata))]
    (schema/validate-matrix-envelope! envelope)))

(defrecord ValhallaRouter [http-client user-agent]
  p/RoutingProtocol
  (route [this request opts]
    (let [request (schema/validate-route-request! request)
          response (request! this "/route" (route-request-body request) opts)
          body (response->json response)]
      (cond
        (and (<= 200 (:status response) 299)
             (map? (:trip body))
             (= 0 (get-in body [:trip :status] 0)))
        (route-envelope request (or body {}))

        (no-route-error? body)
        (empty-route-envelope body)

        :else
        (response-error "Valhalla route request failed" response body))))

  (matrix [this request opts]
    (let [request (schema/validate-matrix-request! request)
          response (request! this "/sources_to_targets" (matrix-request-body request) opts)
          body (response->json response)
          nested-matrix (:sources_to_targets body)]
      (cond
        (and (<= 200 (:status response) 299)
             (or (vector? (:durations body))
                 (vector? (:distances body))
                 (vector? nested-matrix)
                 (and (map? nested-matrix)
                      (or (vector? (:durations nested-matrix))
                          (vector? (:distances nested-matrix))))))
        (matrix-envelope request (or body {}))

        (no-matrix-error? body)
        (empty-matrix-envelope request body)

        :else
        (response-error "Valhalla matrix request failed" response body)))))

(defmethod ig/init-key :d-core.core.routing.valhalla/router
  [_ {:keys [http-client user-agent] :as opts}]
  (when-not http-client
    (throw (ex-info "Valhalla router requires :http-client" {:opts opts})))
  (->ValhallaRouter http-client
                    (or user-agent "d-core routing/valhalla")))
