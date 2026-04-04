(ns d-core.core.ai.schema
  "Canonical schemas for D-core AI generation.

  The public contract is intentionally small:
  - provider-neutral multimodal generation requests
  - normalized generation result envelopes
  - normalized provider capabilities envelopes"
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

(defn- normalize-keyword
  [value]
  (cond
    (keyword? value) value
    (string? value) (-> value
                        str/trim
                        str/lower-case
                        (str/replace #"[_\s]+" "-")
                        keyword)
    :else value))

(defn- normalize-type
  [value]
  (let [type* (normalize-keyword value)]
    (case type*
      :image-url :image-url
      :image-url-part :image-url
      :image-url-content :image-url
      :image-url-input :image-url
      :image-urls :image-url
      :image-urls-input :image-url
      :image_url :image-url
      :image-base64 :image-base64
      :image_base64 :image-base64
      :json-schema :json-schema
      :json_schema :json-schema
      type*)))

(def non-blank-string-schema
  [:and string?
   [:fn {:error/message "expected a non-blank string"}
    (fn [value] (non-blank-string? value))]])

(def role-schema
  [:enum :system :developer :user :assistant :tool])

(def text-content-part-schema
  [:map
   [:type [:= :text]]
   [:text non-blank-string-schema]])

(def image-url-content-part-schema
  [:map
   [:type [:= :image-url]]
   [:url non-blank-string-schema]])

(def image-base64-content-part-schema
  [:map
   [:type [:= :image-base64]]
   [:data non-blank-string-schema]
   [:media-type {:optional true} non-blank-string-schema]])

(def content-part-schema
  [:or
   text-content-part-schema
   image-url-content-part-schema
   image-base64-content-part-schema])

(def message-schema
  [:map
   [:role role-schema]
   [:content [:vector {:min 1} content-part-schema]]
   [:name {:optional true} non-blank-string-schema]
   [:tool-call-id {:optional true} non-blank-string-schema]
   [:metadata {:optional true} map?]])

(def output-schema
  [:or
   [:map
    [:type [:= :text]]]
   [:map
    [:type [:= :json-schema]]
    [:schema map?]
    [:strict? {:optional true} boolean?]
    [:name {:optional true} non-blank-string-schema]]])

(def sampling-schema
  [:map
   [:temperature {:optional true} number?]
   [:top-p {:optional true} number?]
   [:presence-penalty {:optional true} number?]
   [:frequency-penalty {:optional true} number?]
   [:seed {:optional true} int?]])

(def limits-schema
  [:map
   [:max-output-tokens {:optional true} pos-int?]
   [:timeout-ms {:optional true} pos-int?]])

(def generate-request-schema
  [:map
   [:model {:optional true} non-blank-string-schema]
   [:messages [:vector {:min 1} message-schema]]
   [:output {:optional true} output-schema]
   [:sampling {:optional true} sampling-schema]
   [:limits {:optional true} limits-schema]
   [:metadata {:optional true} map?]])

(def usage-schema
  [:map
   [:input-tokens {:optional true} nat-int?]
   [:output-tokens {:optional true} nat-int?]
   [:total-tokens {:optional true} nat-int?]])

(def ^:private generate-item-has-content?
  [:fn {:error/message "generation item requires :text or :structured"}
   (fn [value]
     (or (string? (:text value))
         (map? (:structured value))))])

(def generate-item-schema
  [:and
   [:map
    [:text {:optional true} string?]
    [:structured {:optional true} map?]
    [:finish-reason {:optional true} keyword?]
    [:index {:optional true} nat-int?]
    [:role {:optional true} keyword?]
    [:provider {:optional true} map?]
    [:metadata {:optional true} map?]]
   generate-item-has-content?])

(def generate-envelope-schema
  [:map
   [:items [:vector generate-item-schema]]
   [:provider keyword?]
   [:usage {:optional true} usage-schema]
   [:metadata {:optional true} map?]])

(def capabilities-model-schema
  [:map
   [:id non-blank-string-schema]
   [:input-modalities [:set keyword?]]
   [:output-types [:set keyword?]]
   [:structured-output? {:optional true} boolean?]
   [:vision? {:optional true} boolean?]])

(def capabilities-envelope-schema
  [:map
   [:provider keyword?]
   [:input-modalities [:set keyword?]]
   [:output-types [:set keyword?]]
   [:streaming? boolean?]
   [:structured-output? boolean?]
   [:vision? boolean?]
   [:models {:optional true} [:vector capabilities-model-schema]]
   [:metadata {:optional true} map?]])

(defn- normalize-content-part
  [part]
  (let [part' (-> part
                  strip-nil-values
                  (update :type normalize-type))]
    (cond-> part'
      (= :image-base64 (:type part'))
      (update :media-type (fn [value]
                            (when value (str/lower-case value)))))))

(defn- normalize-message
  [message]
  (let [message' (-> message
                     strip-nil-values
                     (update :role normalize-keyword))]
    (update message' :content
            (fn [parts]
              (mapv normalize-content-part (or parts []))))))

(defn- normalize-output
  [output]
  (let [output' (-> output
                    strip-nil-values
                    (update :type normalize-type))]
    (cond-> output'
      (= :json-schema (:type output'))
      (update :strict? #(if (nil? %) true %)))))

(defn- normalize-generate-item
  [item]
  (let [item' (strip-nil-values item)
        item' (cond-> item'
                (contains? item' :finish-reason)
                (update :finish-reason normalize-keyword)
                (contains? item' :role)
                (update :role normalize-keyword))]
    (strip-nil-values item')))

(defn- normalize-usage
  [usage]
  (strip-nil-values usage))

(defn- normalize-modalities
  [values default]
  (let [normalized (->> values
                        (map normalize-keyword)
                        (remove nil?)
                        set)]
    (if (seq normalized) normalized default)))

(defn- normalize-model-capabilities
  [model]
  (let [model' (strip-nil-values model)]
    (cond-> model'
      true (update :input-modalities #(normalize-modalities % #{:text}))
      true (update :output-types #(normalize-modalities % #{:text}))
      (nil? (:structured-output? model')) (assoc :structured-output? false)
      (nil? (:vision? model')) (assoc :vision? false))))

(defn normalize-generate-request
  [request]
  (let [request' (strip-nil-values request)]
    (cond-> request'
      true (update :messages (fn [messages]
                               (mapv normalize-message (or messages []))))
      true (update :output #(normalize-output (or % {:type :text}))))))

(defn normalize-generate-envelope
  [envelope]
  (let [envelope' (strip-nil-values envelope)]
    (cond-> envelope'
      true (update :items (fn [items]
                            (mapv normalize-generate-item (or items []))))
      (:usage envelope') (update :usage normalize-usage))))

(defn normalize-capabilities-envelope
  [envelope]
  (let [envelope' (strip-nil-values envelope)]
    (cond-> envelope'
      true (update :input-modalities #(normalize-modalities % #{:text}))
      true (update :output-types #(normalize-modalities % #{:text}))
      (nil? (:streaming? envelope')) (assoc :streaming? false)
      (nil? (:structured-output? envelope')) (assoc :structured-output? false)
      (nil? (:vision? envelope')) (assoc :vision? false)
      (:models envelope') (update :models (fn [models]
                                            (mapv normalize-model-capabilities (or models [])))))))

(defn validate-generate-request!
  "Validate and normalize a generation request."
  ([value] (validate-generate-request! value {}))
  ([value opts]
   (let [value' (normalize-generate-request value)]
     (schema/validate! generate-request-schema value'
                       (merge {:schema-id :ai/generate-request
                               :strictness :strict}
                              opts)))))

(defn validate-generate-envelope!
  "Validate and normalize a generation result envelope."
  ([value] (validate-generate-envelope! value {}))
  ([value opts]
   (let [value' (normalize-generate-envelope value)]
     (schema/validate! generate-envelope-schema value'
                       (merge {:schema-id :ai/generate-result
                               :strictness :strict}
                              opts)))))

(defn validate-capabilities-envelope!
  "Validate and normalize a capabilities result envelope."
  ([value] (validate-capabilities-envelope! value {}))
  ([value opts]
   (let [value' (normalize-capabilities-envelope value)]
     (schema/validate! capabilities-envelope-schema value'
                       (merge {:schema-id :ai/capabilities
                               :strictness :strict}
                              opts)))))
