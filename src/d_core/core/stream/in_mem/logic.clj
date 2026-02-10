(ns d-core.core.stream.in-mem.logic
  (:require [clojure.string :as str]))

(defn parse-id [id]
  (if (and id (str/includes? id "-"))
    (let [[ts-str seq-str] (str/split id #"-")]
      [(Long/parseLong ts-str) (Long/parseLong seq-str)])
    [0 -1]))

(defn next-id [last-id now]
  (let [[last-ts last-seq] (parse-id last-id)
        new-ts (max now last-ts)
        new-seq (if (= new-ts last-ts) (inc last-seq) 0)]
    (str new-ts "-" new-seq)))

(defn query-entries [entries {:keys [direction cursor limit]}]
  (let [entries (or entries (sorted-map))
        it (if (= direction :forward)
             (if cursor (subseq entries > cursor) entries)
             (if cursor (rsubseq entries < cursor) (rseq entries)))
        limit-val (long (or limit 50))
        result (->> it
                    (take limit-val)
                    (map (fn [item]
                           (let [[id payload] (if (map-entry? item)
                                                [(key item) (val item)]
                                                [(nth item 0) (nth item 1)])]
                             {:id id :payload payload})))
                    vec)]
    {:entries result
     :next-cursor (when (and (seq result) (= (count result) limit-val))
                    (:id (last result)))}))

(defn append-entries-state [state stream payloads now]
  (if (empty? payloads)
    state
    (let [last-id (get-in state [:last-ids stream] "0-0")
          ids (reduce (fn [acc _]
                        (conj acc (next-id (or (last acc) last-id) now)))
                      []
                      payloads)
          updates (zipmap ids payloads)]
      (-> state
          (update-in [:streams stream] (fnil merge (sorted-map)) updates)
          (assoc-in [:last-ids stream] (last ids))))))

(defn notify-waiters! [state-atom stream]
  (let [waiters (get-in @state-atom [:waiters stream])]
    (when (seq waiters)
      (swap! state-atom update-in [:waiters stream] empty)
      (doseq [w waiters]
        (deliver w true)))))

(defn- read-once [state-atom stream opts]
  (let [state @state-atom
        entries (get-in state [:streams stream])
        res (query-entries entries opts)]
    (if (and (empty? (:entries res)) (:timeout opts))
      ::retry
      res)))

(defn read-payloads-with-blocking [state-atom stream {:keys [timeout] :as opts}]
  (let [res (read-once state-atom stream opts)]
    (if (identical? res ::retry)
      (let [p (promise)
            _ (swap! state-atom update-in [:waiters stream] (fnil conj []) p)
            ;; Check again after registering promise to avoid race condition
            res2 (read-once state-atom stream opts)]
        (if (identical? res2 ::retry)
          (if (and (number? timeout) (pos? timeout))
            (if (deref p timeout nil)
              (read-once state-atom stream (dissoc opts :timeout))
              {:entries [] :next-cursor nil})
            (do (deref p) 
                (read-once state-atom stream (dissoc opts :timeout))))
          res2))
      res)))
