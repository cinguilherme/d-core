(ns d-core.core.geocoding.schema
  "Canonical schemas for D-core geocoding.

  The public contract is intentionally small:
  - free-form or structured geocode inputs
  - reverse-geocode coordinate inputs
  - normalized result envelopes that preserve provider metadata without
    exposing provider-specific wire shapes"
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

(defn- normalize-country-codes
  [codes]
  (when (seq codes)
    (->> codes
         (map str/trim)
         (remove str/blank?)
         (map str/lower-case)
         distinct
         vec)))

(defn- normalize-address
  [address]
  (let [address' (-> address
                     strip-nil-values
                     not-empty)]
    (when address'
      address')))

(def ^:private address-present?
  [:fn {:error/message "address must contain at least one component"}
   (fn [value]
     (boolean (seq value)))])

(def non-blank-string-schema
  [:and string?
   [:fn {:error/message "expected a non-blank string"}
    (fn [value] (non-blank-string? value))]])

(def address-components-schema
  [:and
   [:map
    [:house-number {:optional true} non-blank-string-schema]
    [:street {:optional true} non-blank-string-schema]
    [:city {:optional true} non-blank-string-schema]
    [:county {:optional true} non-blank-string-schema]
    [:state {:optional true} non-blank-string-schema]
    [:country {:optional true} non-blank-string-schema]
    [:postal-code {:optional true} non-blank-string-schema]]
   address-present?])

(def geocode-query-schema
  [:or
   [:map
    [:text non-blank-string-schema]
    [:limit {:optional true} pos-int?]
    [:language {:optional true} non-blank-string-schema]
    [:country-codes {:optional true} [:vector non-blank-string-schema]]]
   [:map
    [:address address-components-schema]
    [:limit {:optional true} pos-int?]
    [:language {:optional true} non-blank-string-schema]
    [:country-codes {:optional true} [:vector non-blank-string-schema]]]])

(def point-schema
  [:map
   [:lat number?]
   [:lon number?]])

(def reverse-geocode-input-schema
  [:map
   [:lat number?]
   [:lon number?]
   [:language {:optional true} non-blank-string-schema]])

(def bounds-schema
  [:map
   [:minlat number?]
   [:minlon number?]
   [:maxlat number?]
   [:maxlon number?]])

(def provider-schema
  [:map
   [:name keyword?]
   [:place-id {:optional true} string?]
   [:osm-type {:optional true} string?]
   [:osm-id {:optional true} string?]
   [:licence {:optional true} string?]
   [:raw {:optional true} map?]])

(def result-components-schema
  [:map
   [:house-number {:optional true} string?]
   [:street {:optional true} string?]
   [:district {:optional true} string?]
   [:city {:optional true} string?]
   [:county {:optional true} string?]
   [:state {:optional true} string?]
   [:country {:optional true} string?]
   [:country-code {:optional true} string?]
   [:postal-code {:optional true} string?]])

(def result-item-schema
  [:map
   [:formatted-address {:optional true} string?]
   [:location point-schema]
   [:components result-components-schema]
   [:bounds {:optional true} bounds-schema]
   [:provider provider-schema]
   [:metadata {:optional true} map?]])

(def result-envelope-schema
  [:map
   [:items [:vector result-item-schema]]
   [:provider keyword?]
   [:metadata {:optional true} map?]])

(defn normalize-geocode-query
  [query]
  (let [query' (strip-nil-values query)]
    (cond-> query'
      (:address query') (update :address normalize-address)
      (:country-codes query') (update :country-codes normalize-country-codes))))

(defn normalize-reverse-geocode-input
  [location]
  (strip-nil-values location))

(defn normalize-result-envelope
  [envelope]
  (let [normalized (strip-nil-values envelope)]
    (update normalized :items
            (fn [items]
              (mapv (fn [item]
                      (cond-> (update item :components #(or % {}))
                        (nil? (:components item)) (assoc :components {})))
                    (or items []))))))

(defn validate-geocode-query!
  "Validate and normalize a geocoding query."
  ([value] (validate-geocode-query! value {}))
  ([value opts]
   (let [value' (normalize-geocode-query value)]
     (schema/validate! geocode-query-schema value'
                       (merge {:schema-id :geocoding/query
                               :strictness :strict}
                              opts)))))

(defn validate-reverse-geocode-input!
  "Validate and normalize a reverse-geocode input map."
  ([value] (validate-reverse-geocode-input! value {}))
  ([value opts]
   (let [value' (normalize-reverse-geocode-input value)]
     (schema/validate! reverse-geocode-input-schema value'
                       (merge {:schema-id :geocoding/reverse-input
                               :strictness :strict}
                              opts)))))

(defn validate-result-envelope!
  "Validate and normalize a provider result envelope."
  ([value] (validate-result-envelope! value {}))
  ([value opts]
   (let [value' (normalize-result-envelope value)]
     (schema/validate! result-envelope-schema value'
                       (merge {:schema-id :geocoding/result
                               :strictness :strict}
                              opts)))))
