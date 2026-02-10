(ns d-core.core.stream.redis.logic
  (:require [clojure.string :as str]))

(def default-limit 50)
(def default-scan-count 100)
(def default-meta-prefix "__dcore:stream")

(defn normalize-direction
  [direction]
  (if (= direction :backward) :backward :forward))

(defn normalize-limit
  [limit]
  (long (or limit default-limit)))

(defn range-start
  [cursor]
  (if cursor (str "(" cursor) "-"))

(defn revrange-end
  [cursor]
  (if cursor (str "(" cursor) "+"))

(defn fields->map
  [fields]
  (cond
    (map? fields) fields
    (sequential? fields) (if (even? (count fields))
                           (apply hash-map fields)
                           {})
    :else {}))

(defn entry->payload
  [fields]
  (let [m (fields->map fields)]
    (or (get m "payload")
        (get m :payload))))

(defn range-entry->map
  [[id fields]]
  {:id id
   :payload (entry->payload fields)})

(defn range-response->entries
  [response]
  (->> (or response [])
       (mapv range-entry->map)))

(defn xread-response->entries
  [response]
  (->> (or response [])
       (mapcat (fn [[_stream entries]]
                 entries))
       (mapv range-entry->map)))

(defn next-cursor
  [entries limit]
  (let [limit-val (normalize-limit limit)]
    (when (and (seq entries) (= (count entries) limit-val))
      (:id (last entries)))))

(defn read-result
  [entries limit]
  {:entries entries
   :next-cursor (next-cursor entries limit)})

(defn blocking-request?
  [{:keys [direction timeout]} read-once-result]
  (and (= :forward (normalize-direction direction))
       (number? timeout)
       (pos? timeout)
       (empty? (:entries read-once-result))))

(defn block-start-id
  [cursor]
  (or cursor "0-0"))

(defn parse-id
  [id]
  (when (and id (str/includes? id "-"))
    (let [parts (str/split id #"-")]
      (when (= 2 (count parts))
        (let [[ts-str seq-str] parts]
        (try
          [(Long/parseLong ts-str)
           (Long/parseLong seq-str)]
          (catch NumberFormatException _
              nil)))))))

(defn next-stream-id
  [id]
  (when-let [[ts seq] (parse-id id)]
    (str ts "-" (inc seq))))

(defn trim-min-id
  [id]
  (next-stream-id id))

(defn normalize-meta-prefix
  [meta-prefix]
  (or meta-prefix default-meta-prefix))

(defn cursor-hash-key
  [meta-prefix]
  (str (normalize-meta-prefix meta-prefix) ":cursors"))

(defn sequence-hash-key
  [meta-prefix]
  (str (normalize-meta-prefix meta-prefix) ":sequences"))

(defn parse-scan-response
  [response]
  (let [[next-cursor keys] (or response ["0" []])]
    {:next-cursor (or next-cursor "0")
     :keys (vec (or keys []))}))

(defn continue-scan?
  [cursor]
  (not= "0" cursor))
