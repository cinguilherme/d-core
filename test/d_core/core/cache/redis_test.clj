(ns d-core.core.cache.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.protocol :as p]
            [d-core.core.cache.redis :as redis]))

(deftest redis-cache-lookup
  (testing "Redis cache lookup delegates to redis-get"
    (let [calls (atom [])
          c (redis/->RedisCache :redis)]
      (with-redefs [d-core.core.cache.redis/redis-get (fn [client key]
                                                        (swap! calls conj {:client client :key key})
                                                        :hit)]
        (is (= :hit (p/cache-lookup c "k" nil)))
        (is (= [{:client :redis :key "k"}] @calls))))))

(deftest redis-cache-put
  (testing "Redis cache put uses setex when ttl is provided"
    (let [calls (atom [])
          c (redis/->RedisCache :redis)]
      (with-redefs [d-core.core.cache.redis/redis-set (fn [client key value]
                                                        (swap! calls conj {:op :set :client client :key key :value value})
                                                        :ok)
                    d-core.core.cache.redis/redis-setex (fn [client key ttl value]
                                                          (swap! calls conj {:op :setex :client client :key key :ttl ttl :value value})
                                                          :ok-ttl)]
        (is (= :ok (p/cache-put c "k1" "v1" {})))
        (is (= :ok-ttl (p/cache-put c "k2" "v2" {:ttl 10})))
        (is (= [{:op :set :client :redis :key "k1" :value "v1"}
                {:op :setex :client :redis :key "k2" :ttl 10 :value "v2"}]
               @calls))))))

(deftest redis-cache-delete
  (testing "Redis cache delete delegates to redis-del"
    (let [calls (atom [])
          c (redis/->RedisCache :redis)]
      (with-redefs [d-core.core.cache.redis/redis-del (fn [client key]
                                                        (swap! calls conj {:client client :key key})
                                                        :gone)]
        (is (= :gone (p/cache-delete c "k" nil)))
        (is (= [{:client :redis :key "k"}] @calls))))))

(deftest redis-cache-clear
  (testing "Redis cache clear delegates to redis-flushdb"
    (let [calls (atom [])
          c (redis/->RedisCache :redis)]
      (with-redefs [d-core.core.cache.redis/redis-flushdb (fn [client]
                                                            (swap! calls conj {:client client})
                                                            :ok)]
        (is (= :ok (p/cache-clear c nil)))
        (is (= [{:client :redis}] @calls))))))
