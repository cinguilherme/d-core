(ns d-core.core.stream.redis.redis
  (:require [d-core.core.stream.protocol :as p-streams]
            [d-core.core.stream.redis.logic :as logic]
            [integrant.core :as ig]
            [taoensso.carmine :as car]
            [d-core.core.stream.common :as common]
            [d-core.core.clients.redis.client]))

(defn redis-xadd
  [redis-client stream payload]
  (car/wcar (:conn redis-client)
    (car/xadd stream "*" "payload" payload)))

(defn redis-xrange
  [redis-client stream start end limit]
  (car/wcar (:conn redis-client)
    (car/xrange stream start end "COUNT" (str limit))))

(defn redis-xrevrange
  [redis-client stream end start limit]
  (car/wcar (:conn redis-client)
    (car/xrevrange stream end start "COUNT" (str limit))))

(defn redis-xread-block
  [redis-client stream start-id timeout-ms limit]
  (car/wcar (:conn redis-client)
    (car/xread "BLOCK" (str timeout-ms)
               "COUNT" (str limit)
               "STREAMS" stream start-id)))

(defn redis-xtrim-minid
  [redis-client stream min-id]
  (car/wcar (:conn redis-client)
    (car/xtrim stream "MINID" min-id)))

(defn redis-scan-streams
  [redis-client cursor pattern scan-count]
  (car/wcar (:conn redis-client)
    (car/scan cursor
              "MATCH" pattern
              "TYPE" "stream"
              "COUNT" (str scan-count))))

(defn redis-hget
  [redis-client hash-key field]
  (car/wcar (:conn redis-client)
    (car/hget hash-key field)))

(defn redis-hset
  [redis-client hash-key field value]
  (car/wcar (:conn redis-client)
    (car/hset hash-key field value)))

(defn redis-hincrby
  [redis-client hash-key field amount]
  (car/wcar (:conn redis-client)
    (car/hincrby hash-key field amount)))

(defn- read-once
  [redis-client stream {:keys [direction cursor limit]}]
  (let [direction (logic/normalize-direction direction)
        limit-val (logic/normalize-limit limit)
        response (if (= direction :forward)
                   (redis-xrange redis-client
                                 stream
                                 (logic/range-start cursor)
                                 "+"
                                 limit-val)
                   (redis-xrevrange redis-client
                                    stream
                                    (logic/revrange-end cursor)
                                    "-"
                                    limit-val))
        entries (logic/range-response->entries response)]
    (logic/read-result entries limit-val)))

(defrecord RedisStreamBackend [redis-client meta-prefix scan-count]
  p-streams/StreamBackend
  (append-payload! [_ stream payload-bytes]
    (redis-xadd redis-client stream payload-bytes))

  (append-batch! [_ stream payloads-bytes]
    (mapv #(redis-xadd redis-client stream %) payloads-bytes))

  (read-payloads [_ stream opts]
    (let [opts (update opts :direction logic/normalize-direction)
          first-read (read-once redis-client stream opts)]
      (if (logic/blocking-request? opts first-read)
        (let [timeout-ms (:timeout opts)
              limit-val (logic/normalize-limit (:limit opts))
              start-id (logic/block-start-id (:cursor opts))
              response (redis-xread-block redis-client
                                          stream
                                          start-id
                                          timeout-ms
                                          limit-val)
              entries (logic/xread-response->entries response)]
          (if (seq entries)
            (logic/read-result entries limit-val)
            {:entries [] :next-cursor nil}))
        first-read)))

  (trim-stream! [_ stream id]
    (when-let [min-id (logic/trim-min-id id)]
      (redis-xtrim-minid redis-client stream min-id)))

  (list-streams [_ pattern]
    (loop [cursor "0"
           acc #{}]
      (let [response (redis-scan-streams redis-client cursor pattern scan-count)
            {:keys [next-cursor keys]} (logic/parse-scan-response response)
            next-acc (into acc keys)]
        (if (logic/continue-scan? next-cursor)
          (recur next-cursor next-acc)
          (sort next-acc)))))

  (get-cursor [_ key]
    (redis-hget redis-client (logic/cursor-hash-key meta-prefix) key))

  (set-cursor! [_ key cursor]
    (redis-hset redis-client (logic/cursor-hash-key meta-prefix) key cursor))

  (next-sequence! [_ key]
    (redis-hincrby redis-client (logic/sequence-hash-key meta-prefix) key 1)))

(defmethod ig/init-key :core-service.app.streams.redis/backend
  [_ {:keys [redis-client meta-key-prefix scan-count logger metrics]
      :or {scan-count logic/default-scan-count}}]
  (let [backend (->RedisStreamBackend redis-client
                                      (logic/normalize-meta-prefix meta-key-prefix)
                                      scan-count)]
    (common/wrap-backend backend logger metrics)))
