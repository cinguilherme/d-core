(ns d-core.core.schema
  "Schema helpers for D-core.

  This module is intentionally small:
  - Applies strictness (:strict/:lax) to Malli map schemas (:closed true/false)
  - Validates values and throws typed, non-retriable exceptions for poison handling."
  (:require [clojure.walk :as walk]
            [malli.core :as m]
            [malli.error :as me]))

(defn- strictness->closed
  [strictness]
  (case (or strictness :lax)
    :strict true
    :lax false
    ;; Default: be permissive on consumers unless explicitly strict.
    false))

(defn- map-schema?
  [x]
  (and (vector? x) (= :map (first x))))

(defn- set-map-closed
  "Ensures a :map schema has an options map and sets {:closed <bool>}."
  [schema closed?]
  (let [[tag maybe-opts & more] schema]
    (cond
      (not= tag :map) schema
      (map? maybe-opts) (into [:map (assoc maybe-opts :closed closed?)] more)
      :else (into [:map {:closed closed?} maybe-opts] more))))

(defn apply-strictness
  "Applies :strict/:lax strictness to all Malli :map schemas by setting :closed."
  [schema strictness]
  (let [closed? (strictness->closed strictness)]
    (walk/postwalk
      (fn [x]
        (if (map-schema? x)
          (set-map-closed x closed?)
          x))
      schema)))

(defn explain
  "Returns a compact explain payload safe to include in ex-data / DLQ.
  Includes both raw explain and humanized errors."
  [schema value]
  (let [s (m/schema schema)
        ex (m/explain s value)]
    {:errors (me/humanize ex)
     :explain ex}))

(defn validate!
  "Validates `value` against `schema` (optionally applying `strictness`).

  On failure, throws ex-info with:
  - :failure/type :schema-invalid
  - :retriable? false
  - :schema/id (optional)
  - :schema/explain (compact explain payload)"
  ([schema value] (validate! schema value {}))
  ([schema value {:keys [schema-id strictness]}]
   (let [schema (if strictness (apply-strictness schema strictness) schema)
         s (m/schema schema)]
     (when-not (m/validate s value)
       (throw (ex-info "Schema validation failed"
                       {:failure/type :schema-invalid
                        :retriable? false
                        :schema/id schema-id
                        :schema/explain (explain schema value)})))
     value)))

