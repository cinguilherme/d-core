(ns d-core.core.geocoding.nominatim
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.core.geocoding.protocol :as p]
            [d-core.core.geocoding.schema :as schema]
            [d-core.core.http.client :as http]
            [integrant.core :as ig]))

(defn- parse-number
  [value]
  (cond
    (number? value) (double value)
    (string? value) (Double/parseDouble value)
    :else nil))

(defn- point-from-geometry
  [geometry]
  (let [coords (:coordinates geometry)]
    (when-not (and (= "Point" (:type geometry))
                   (vector? coords)
                   (<= 2 (count coords)))
      (throw (ex-info "Nominatim geocodejson feature is missing a Point geometry"
                      {:geometry geometry})))
    (let [[lon lat] coords]
      {:lat (parse-number lat)
       :lon (parse-number lon)})))

(defn- bounds-from-feature
  [feature]
  (cond
    (and (vector? (:bbox feature))
         (= 4 (count (:bbox feature))))
    (let [[minlon minlat maxlon maxlat] (:bbox feature)]
      {:minlat (parse-number minlat)
       :minlon (parse-number minlon)
       :maxlat (parse-number maxlat)
       :maxlon (parse-number maxlon)})

    (and (vector? (:boundingbox feature))
         (= 4 (count (:boundingbox feature))))
    (let [[minlat maxlat minlon maxlon] (:boundingbox feature)]
      {:minlat (parse-number minlat)
       :minlon (parse-number minlon)
       :maxlat (parse-number maxlat)
       :maxlon (parse-number maxlon)})

    :else nil))

(defn- non-empty-map
  [value]
  (when (and (map? value) (seq value))
    value))

(defn- feature->result-item
  [feature licence]
  (let [geocoding (get-in feature [:properties :geocoding] {})
        metadata (non-empty-map
                  {:name (:name geocoding)
                   :type (:type geocoding)
                   :accuracy (:accuracy geocoding)
                   :osm-key (:osm_key geocoding)
                   :osm-value (:osm_value geocoding)
                   :locality (:locality geocoding)
                   :admin (:admin geocoding)})]
    {:formatted-address (:label geocoding)
     :location (point-from-geometry (:geometry feature))
     :components {:house-number (:housenumber geocoding)
                  :street (:street geocoding)
                  :district (:district geocoding)
                  :city (:city geocoding)
                  :county (:county geocoding)
                  :state (:state geocoding)
                  :country (:country geocoding)
                  :country-code (some-> (:country_code geocoding) str/lower-case)
                  :postal-code (:postcode geocoding)}
     :bounds (bounds-from-feature feature)
     :provider {:name :nominatim
                :place-id (some-> (:place_id geocoding) str)
                :osm-type (some-> (:osm_type geocoding) str)
                :osm-id (some-> (:osm_id geocoding) str)
                :licence licence
                :raw feature}
     :metadata metadata}))

(defn- response->json
  [resp]
  (let [body (:body resp)]
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

(defn- request!
  [{:keys [http-client user-agent email]} path query-params opts]
  (let [request {:method :get
                 :path path
                 :as :text
                 :headers {"Accept" "application/json"
                           "User-Agent" user-agent}
                 :query-params (cond-> query-params
                                 email (assoc :email email))}]
    (http/request! http-client
                   (cond-> request
                     (:request opts) (merge (:request opts))))))

(defn- feature-seq
  [body]
  (let [features (:features body)]
    (cond
      (vector? features) features
      (map? features) [features]
      (= "Feature" (:type body)) [body]
      :else [])))

(defn- envelope-metadata
  [body]
  (non-empty-map
   {:version (get-in body [:geocoding :version])
    :attribution (get-in body [:geocoding :attribution])
    :licence (get-in body [:geocoding :licence])
    :query (get-in body [:geocoding :query])}))

(defn- empty-envelope
  [provider metadata]
  (schema/validate-result-envelope!
   (cond-> {:items []
            :provider provider}
     metadata (assoc :metadata metadata))))

(defn- normalize-envelope
  [body]
  (let [metadata (envelope-metadata body)
        licence (or (:licence metadata)
                    (get-in body [:geocoding :licence]))]
    (schema/validate-result-envelope!
     (cond-> {:items (mapv #(feature->result-item % licence) (feature-seq body))
              :provider :nominatim}
       metadata (assoc :metadata metadata)))))

(defn- geocode-query->params
  [{:keys [text address limit language country-codes]}]
  (cond-> {:format "geocodejson"
           :addressdetails 1}
    text (assoc :q text)
    address (merge (cond-> {}
                     (:street address)
                     (assoc :street (str/join " "
                                              (remove str/blank?
                                                      [(:house-number address)
                                                       (:street address)])))
                     (:city address) (assoc :city (:city address))
                     (:county address) (assoc :county (:county address))
                     (:state address) (assoc :state (:state address))
                     (:country address) (assoc :country (:country address))
                     (:postal-code address) (assoc :postalcode (:postal-code address))))
    limit (assoc :limit limit)
    language (assoc :accept-language language)
    (seq country-codes) (assoc :countrycodes (str/join "," country-codes))))

(defn- reverse-query->params
  [{:keys [lat lon language]}]
  (cond-> {:format "geocodejson"
           :addressdetails 1
           :lat lat
           :lon lon}
    language (assoc :accept-language language)))

(defn- no-result-error?
  [body]
  (when-let [message (:error body)]
    (boolean (re-find #"(?i)(unable to geocode|no address found|no data coverage)"
                      message))))

(defrecord NominatimGeocoder [http-client user-agent email]
  p/GeocodingProtocol
  (geocode [this query opts]
    (let [query (schema/validate-geocode-query! query)
          response (request! this "/search" (geocode-query->params query) opts)
          body (response->json response)]
      (if (<= 200 (:status response) 299)
        (normalize-envelope (or body {}))
        (response-error "Nominatim geocode request failed" response body))))

  (reverse-geocode [this location opts]
    (let [location (schema/validate-reverse-geocode-input! location)
          response (request! this "/reverse" (reverse-query->params location) opts)
          body (response->json response)
          metadata (envelope-metadata (or body {}))]
      (cond
        (<= 200 (:status response) 299)
        (if (no-result-error? body)
          (empty-envelope :nominatim metadata)
          (normalize-envelope (or body {})))

        (= 404 (:status response))
        (empty-envelope :nominatim metadata)

        :else
        (response-error "Nominatim reverse-geocode request failed" response body)))))

(defmethod ig/init-key :d-core.core.geocoding.nominatim/geocoder
  [_ {:keys [http-client user-agent email] :as opts}]
  (when-not http-client
    (throw (ex-info "Nominatim geocoder requires :http-client" {:opts opts})))
  (->NominatimGeocoder http-client
                       (or user-agent "d-core geocoder")
                       email))
