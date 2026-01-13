(ns d-core.queue
  (:refer-clojure :exclude [peek])
  (:require [integrant.core :as ig]))

;; Integrant-managed, in-memory FIFO queue.
;;
;; We store a PersistentQueue inside an atom so we can update it atomically.
(defmethod ig/init-key :d-core.queue/in-memory-queue
  [_ _opts]
  (atom clojure.lang.PersistentQueue/EMPTY))

(defmethod ig/init-key :d-core.queue/in-memory-queues
  [_ {:keys [topics] :or {topics []}}]
  ;; Store the topic->queue map inside an atom so we can lazily create new queues
  ;; (e.g. derived dead-letter topics like :orders.dl) without extra configuration.
  {:topics->queue (atom (into {}
                              (map (fn [topic]
                                     [topic (atom clojure.lang.PersistentQueue/EMPTY)]))
                              topics))})

(defn known-topics
  [queues]
  (set (keys @(:topics->queue queues))))

(defn get-queue
  [queues topic]
  (get @(:topics->queue queues) topic))

(defn get-or-create-queue!
  "Returns the queue atom for `topic`, creating it if missing."
  [queues topic]
  (or (get-queue queues topic)
      (let [q (atom clojure.lang.PersistentQueue/EMPTY)]
        (get (swap! (:topics->queue queues) #(if (contains? % topic) % (assoc % topic q)))
             topic))))

(defn get-queue!
  [queues topic]
  (get-or-create-queue! queues topic))

(defn enqueue!
  [queue item]
  (swap! queue conj item))

(defn dequeue!
  "Removes and returns the next item from the queue, or nil when empty."
  [queue]
  (let [[old _new] (swap-vals! queue (fn [q] (if (empty? q) q (pop q))))]
    (when-not (empty? old)
      (clojure.core/peek old))))

(defn peek
  "Returns the next item without removing it, or nil when empty."
  [queue]
  (clojure.core/peek @queue))

(defn size
  [queue]
  (count @queue))

;; Backwards-compatible names (for early iteration).
(def enqueue enqueue!)
(def dequeue dequeue!)