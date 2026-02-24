(ns d-core.core.stream.cursor
  (:require [clojure.string :as str]
            [d-core.core.pagination.token :as token]))

(def ^:private allowed-sources #{"redis" "minio"})
(def ^:private allowed-directions #{"forward" "backward"})

(defn- non-blank-string?
  [value]
  (and (string? value) (not (str/blank? value))))

(defn- valid-cursor-type?
  [source cursor]
  (case source
    "redis" (non-blank-string? cursor)
    "minio" (number? cursor)
    false))

(defn- valid-token-shape?
  [decoded]
  (let [source (:source decoded)
        direction (:direction decoded)
        conversation-id (:conversation_id decoded)
        cursor (:cursor decoded)]
    (and (map? decoded)
         (contains? decoded :source)
         (contains? decoded :direction)
         (contains? decoded :conversation_id)
         (contains? decoded :cursor)
         (string? source)
         (contains? allowed-sources source)
         (string? direction)
         (contains? allowed-directions direction)
         (non-blank-string? conversation-id)
         (valid-cursor-type? source cursor))))

(defn encode-cursor
  [{:keys [conversation-id cursor direction source]}]
  (token/encode-token {:conversation_id (str conversation-id)
                       :cursor cursor
                       :direction (name direction)
                       :source (name source)}))

(defn decode-cursor
  [cursor-token]
  (if-not (non-blank-string? cursor-token)
    {:token nil
     :source nil
     :direction nil
     :cursor nil
     :seq-cursor nil
     :conversation-id nil
     :invalid? false}
    (let [decoded (token/decode-token-map cursor-token)]
      (if (valid-token-shape? decoded)
        (let [source (:source decoded)
              direction (:direction decoded)
              cursor (:cursor decoded)
              seq-cursor (when (number? cursor) (long cursor))]
          {:token decoded
           :source (keyword source)
           :direction (keyword direction)
           :cursor cursor
           :seq-cursor seq-cursor
           :conversation-id (:conversation_id decoded)
           :invalid? false})
        {:token nil
         :source nil
         :direction nil
         :cursor nil
         :seq-cursor nil
         :conversation-id nil
         :invalid? true}))))
