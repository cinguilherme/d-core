(ns d-core.core.stream.redis.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.stream.protocol :as p]
            [d-core.core.stream.redis.redis :as redis]))

(defn- make-backend
  []
  (redis/->RedisStreamBackend {:conn :fake} "__test:stream" 100))

(deftest append-operations-test
  (testing "append payload delegates to xadd wrapper"
    (let [calls (atom [])
          backend (make-backend)]
      (with-redefs [d-core.core.stream.redis.redis/redis-xadd
                    (fn [client stream payload]
                      (swap! calls conj {:client client :stream stream :payload payload})
                      "1000-0")]
        (is (= "1000-0" (p/append-payload! backend "s1" "p1")))
        (is (= [{:client {:conn :fake} :stream "s1" :payload "p1"}] @calls)))))

  (testing "append batch returns xadd ids in order"
    (let [calls (atom [])
          backend (make-backend)]
      (with-redefs [d-core.core.stream.redis.redis/redis-xadd
                    (fn [_client stream payload]
                      (swap! calls conj [stream payload])
                      (str payload "-id"))]
        (is (= ["p1-id" "p2-id" "p3-id"]
               (p/append-batch! backend "s2" ["p1" "p2" "p3"])))
        (is (= [["s2" "p1"] ["s2" "p2"] ["s2" "p3"]]
               @calls))))))

(deftest read-operations-test
  (testing "forward read maps xrange response"
    (let [backend (make-backend)]
      (with-redefs [d-core.core.stream.redis.redis/redis-xrange
                    (fn [_client _stream start end limit]
                      (is (= "-" start))
                      (is (= "+" end))
                      (is (= 2 limit))
                      [["1000-0" ["payload" "a"]]
                       ["1000-1" ["payload" "b"]]])]
        (is (= {:entries [{:id "1000-0" :payload "a"}
                          {:id "1000-1" :payload "b"}]
                :next-cursor "1000-1"}
               (p/read-payloads backend "s1" {:direction :forward :limit 2}))))))

  (testing "backward read maps xrevrange response"
    (let [backend (make-backend)]
      (with-redefs [d-core.core.stream.redis.redis/redis-xrevrange
                    (fn [_client _stream end start limit]
                      (is (= "+" end))
                      (is (= "-" start))
                      (is (= 2 limit))
                      [["1000-2" ["payload" "c"]]
                       ["1000-1" ["payload" "b"]]])]
        (is (= {:entries [{:id "1000-2" :payload "c"}
                          {:id "1000-1" :payload "b"}]
                :next-cursor "1000-1"}
               (p/read-payloads backend "s1" {:direction :backward :limit 2}))))))

  (testing "blocking read uses xread when first read is empty"
    (let [backend (make-backend)]
      (with-redefs [d-core.core.stream.redis.redis/redis-xrange
                    (fn [_client _stream _start _end _limit]
                      [])
                    d-core.core.stream.redis.redis/redis-xread-block
                    (fn [_client _stream start-id timeout-ms limit]
                      (is (= "0-0" start-id))
                      (is (= 150 timeout-ms))
                      (is (= 1 limit))
                      [["s1" [["1000-0" ["payload" "late"]]]]])]
        (is (= {:entries [{:id "1000-0" :payload "late"}]
                :next-cursor "1000-0"}
               (p/read-payloads backend "s1" {:timeout 150 :limit 1}))))))

  (testing "blocking timeout returns empty result"
    (let [backend (make-backend)]
      (with-redefs [d-core.core.stream.redis.redis/redis-xrange
                    (fn [_client _stream _start _end _limit]
                      [])
                    d-core.core.stream.redis.redis/redis-xread-block
                    (fn [_client _stream _start-id _timeout-ms _limit]
                      nil)]
        (is (= {:entries [] :next-cursor nil}
               (p/read-payloads backend "s1" {:timeout 50 :limit 1}))))))
  )

(deftest utility-operations-test
  (testing "trim translates strict > id into xtrim MINID next-id"
    (let [calls (atom [])
          backend (make-backend)]
      (with-redefs [d-core.core.stream.redis.redis/redis-xtrim-minid
                    (fn [_client stream min-id]
                      (swap! calls conj {:stream stream :min-id min-id})
                      1)]
        (is (= 1 (p/trim-stream! backend "s1" "1000-0")))
        (is (= [{:stream "s1" :min-id "1000-1"}] @calls)))))

  (testing "cursor and sequence utility operations use redis hashes"
    (let [backend (make-backend)
          hget-calls (atom [])
          hset-calls (atom [])
          hincr-calls (atom [])]
      (with-redefs [d-core.core.stream.redis.redis/redis-hget
                    (fn [_client hash-key field]
                      (swap! hget-calls conj {:hash-key hash-key :field field})
                      "cursor-v")
                    d-core.core.stream.redis.redis/redis-hset
                    (fn [_client hash-key field value]
                      (swap! hset-calls conj {:hash-key hash-key :field field :value value})
                      1)
                    d-core.core.stream.redis.redis/redis-hincrby
                    (fn [_client hash-key field amount]
                      (swap! hincr-calls conj {:hash-key hash-key :field field :amount amount})
                      2)]
        (is (= 1 (p/set-cursor! backend "c1" "v1")))
        (is (= "cursor-v" (p/get-cursor backend "c1")))
        (is (= 2 (p/next-sequence! backend "seq1")))
        (is (= [{:hash-key "__test:stream:cursors" :field "c1" :value "v1"}] @hset-calls))
        (is (= [{:hash-key "__test:stream:cursors" :field "c1"}] @hget-calls))
        (is (= [{:hash-key "__test:stream:sequences" :field "seq1" :amount 1}] @hincr-calls)))))

  (testing "list-streams scans until cursor reaches zero"
    (let [backend (make-backend)
          calls (atom [])]
      (with-redefs [d-core.core.stream.redis.redis/redis-scan-streams
                    (fn [_client cursor pattern scan-count]
                      (swap! calls conj {:cursor cursor :pattern pattern :scan-count scan-count})
                      (if (= cursor "0")
                        ["7" ["user:2:events"]]
                        ["0" ["user:1:events"]]))]
        (is (= ["user:1:events" "user:2:events"]
               (p/list-streams backend "user:*")))
        (is (= [{:cursor "0" :pattern "user:*" :scan-count 100}
                {:cursor "7" :pattern "user:*" :scan-count 100}]
               @calls))))))
