(ns d-core.libs.dtap
  (:require [clojure.string :as str]))

(def ^:private ansi
  {:reset "\u001b[0m"
   :bold "\u001b[1m"
   :dim "\u001b[2m"
   :black "\u001b[30m"
   :red "\u001b[31m"
   :green "\u001b[32m"
   :yellow "\u001b[33m"
   :blue "\u001b[34m"
   :magenta "\u001b[35m"
   :cyan "\u001b[36m"
   :white "\u001b[37m"
   :bright-black "\u001b[90m"
   :bright-red "\u001b[91m"
   :bright-green "\u001b[92m"
   :bright-yellow "\u001b[93m"
   :bright-blue "\u001b[94m"
   :bright-magenta "\u001b[95m"
   :bright-cyan "\u001b[96m"
   :bright-white "\u001b[97m"})

(def ^:private default-colors
  {:header :bright-blue
   :bracket :bright-black
   :map-key :magenta
   :string :green
   :number :yellow
   :keyword :cyan
   :nil :bright-black
   :boolean :yellow
   :symbol :blue
   :default :white})

(def ^:private default-opts
  {:color? true
   :colors default-colors
   :indent 2
   :max-depth nil
   :max-items nil
   :header? true
   :show-form? true
   :prefix "dtap"
   :tap? false
   :out nil})

(defn- resolve-color [colors k]
  (let [c (get colors k)]
    (cond
      (string? c) c
      (keyword? c) (get ansi c "")
      :else "")))

(defn- colorize [opts k s]
  (if-not (:color? opts)
    s
    (let [colors (:colors opts)
          open (resolve-color colors k)
          reset (get ansi :reset "")]
      (if (seq open)
        (str open s reset)
        s))))

(defn- indent-str [opts depth]
  (let [n (* depth (long (:indent opts)))]
    (when (pos? n)
      (apply str (repeat n " ")))))

(defn- scalar-style [v key?]
  (cond
    key? :map-key
    (string? v) :string
    (keyword? v) :keyword
    (number? v) :number
    (nil? v) :nil
    (boolean? v) :boolean
    (symbol? v) :symbol
    :else :default))

(declare format-value)

(defn- coll-like? [v]
  (or (map? v) (vector? v) (set? v) (list? v) (seq? v)))

(defn- limit-items [opts xs]
  (let [max-items (:max-items opts)]
    (if (and max-items (number? max-items))
      (let [taken (take (inc max-items) xs)
            truncated? (> (count taken) max-items)
            items (take max-items taken)]
        [items truncated?])
      [xs false])))

(defn- inline-coll? [opts xs depth truncated? fmt-fn]
  (let [max-count (or (:inline-count opts) 6)
        width (or (:width opts) 80)
        xs (take (inc max-count) xs)
        inline-count-ok? (<= (count xs) max-count)]
    (when inline-count-ok?
      (let [parts (map fmt-fn xs)
            parts (if truncated? (concat parts [(colorize opts :default "...")]) parts)
            joined (str/join " " parts)]
        (and (every? string? parts)
             (every? #(not (str/includes? % "\n")) parts)
             (<= (count joined) width))))))

(defn- format-coll [opts open close xs depth]
  (let [[items truncated?] (limit-items opts xs)
        empty? (and (empty? items) (not truncated?))]
    (if empty?
      (str (colorize opts :bracket open) (colorize opts :bracket close))
      (let [inline? (inline-coll? opts items depth truncated?
                                  #(format-value opts % (inc depth) false))]
        (if inline?
          (str (colorize opts :bracket open)
               (str/join " " (concat (map #(format-value opts % (inc depth) false) items)
                                     (when truncated? [(colorize opts :default "...")])))
               (colorize opts :bracket close))
          (let [inner-indent (indent-str opts (inc depth))
                outer-indent (indent-str opts depth)
                lines (map #(str inner-indent (format-value opts % (inc depth) false)) items)
                lines (if truncated?
                        (concat lines [(str inner-indent (colorize opts :default "..."))])
                        lines)]
            (str (colorize opts :bracket open)
                 "\n"
                 (str/join "\n" lines)
                 "\n"
                 outer-indent
                 (colorize opts :bracket close))))))))

(defn- format-map [opts m depth]
  (let [[entries truncated?] (limit-items opts (seq m))
        empty? (and (empty? entries) (not truncated?))]
    (if empty?
      (str (colorize opts :bracket "{") (colorize opts :bracket "}"))
      (let [inline? (inline-coll? opts entries depth truncated?
                                  (fn [[k v]]
                                    (str (format-value opts k (inc depth) true)
                                         " "
                                         (format-value opts v (inc depth) false))))]
        (if inline?
          (str (colorize opts :bracket "{")
               (str/join
                " "
                (concat
                 (map (fn [[k v]]
                        (str (format-value opts k (inc depth) true)
                             " "
                             (format-value opts v (inc depth) false)))
                      entries)
                 (when truncated? [(colorize opts :default "...")])))
               (colorize opts :bracket "}"))
          (let [inner-indent (indent-str opts (inc depth))
                outer-indent (indent-str opts depth)
                lines (map (fn [[k v]]
                             (str inner-indent
                                  (format-value opts k (inc depth) true)
                                  " "
                                  (format-value opts v (inc depth) false)))
                           entries)
                lines (if truncated?
                        (concat lines [(str inner-indent (colorize opts :default "..."))])
                        lines)]
            (str (colorize opts :bracket "{")
                 "\n"
                 (str/join "\n" lines)
                 "\n"
                 outer-indent
                 (colorize opts :bracket "}"))))))))

(defn- format-value [opts v depth key?]
  (let [max-depth (:max-depth opts)
        depth-cutoff? (and max-depth (number? max-depth) (>= depth max-depth))]
    (cond
      (and depth-cutoff? (coll-like? v)) (colorize opts :default "#<max-depth>")
      (map? v) (format-map opts v depth)
      (vector? v) (format-coll opts "[" "]" v depth)
      (set? v) (format-coll opts "#{" "}" v depth)
      (or (list? v) (seq? v)) (format-coll opts "(" ")" v depth)
      :else (colorize opts (scalar-style v key?) (pr-str v)))))

(defn- header-line [opts form-str]
  (when (and (:header? opts)
             (or (:label opts) (:show-form? opts)))
    (let [label (cond
                  (string? (:label opts)) (:label opts)
                  (some? (:label opts)) (pr-str (:label opts))
                  (:show-form? opts) form-str
                  :else nil)]
      (when label
        (colorize opts :header (str (:prefix opts) ": " label))))))

(defn dtap*
  "Pretty-print a value with colors. Returns the original value.

  Options:
  - :label (any)        Header label (printed with pr-str)
  - :header? (bool)     Enable header line (default true)
  - :show-form? (bool)  Show form when label is not provided (default true)
  - :prefix (string)    Header prefix (default \"dtap\")
  - :indent (int)       Indent size for multiline collections (default 2)
  - :max-depth (int)    Max depth for nested collections (nil = unlimited)
  - :max-items (int)    Max items per collection/map (nil = unlimited)
  - :width (int)        Inline width hint (default 80)
  - :inline-count (int) Max items for inline collections (default 6)
  - :color? (bool)      Enable ANSI colors (default true)
  - :colors (map)       Style overrides (keyword -> color keyword or ANSI string)
                        Styles: :header :bracket :map-key :string :number
                                :keyword :nil :boolean :symbol :default
  - :tap? (bool)        Also send value to tap> (default false)
  - :out (writer)       Print to this writer instead of *out*"
  ([value]
   (dtap* {} value nil))
  ([opts value]
   (dtap* opts value nil))
  ([opts value form-str]
   (let [opts (merge default-opts opts)
         header (header-line opts form-str)
         out (or (:out opts) *out*)]
     (try
       (binding [*out* out]
         (when header
           (println header))
         (println (format-value opts value 0 false)))
       (catch Exception _e
         (binding [*out* out]
           (when header
             (println header))
           (println (pr-str value)))))
     (when (:tap? opts)
       (try
         (tap> value)
         (catch Exception _e
           nil)))
     value)))

(defmacro dtap
  "Macro wrapper for dtap* that captures the original form.

  Usage:
    (dtap expr)
    (dtap opts expr)

  For reusable taps:
    (fn [v] (dtap {:label \"x\"} v))"
  ([expr]
   `(dtap* {} ~expr ~(pr-str expr)))
  ([opts expr]
   `(dtap* ~opts ~expr ~(pr-str expr))))

(comment
  (defn ->map [vec] (zipmap (range) vec))
  (defn dd [v] (dtap {:label "dd" :color :red} v))

  (->> [1 2 3]
       dd
       (map inc)
       dtap
       ->map
       dtap)

  (let [tap-x (fn [v] (dtap {:label "x"} v))]
    (-> {:a 1 :b [2 3]}
         tap-x
         (assoc :c :d)
         tap-x)))
