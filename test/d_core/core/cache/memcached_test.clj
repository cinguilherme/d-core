(ns d-core.core.cache.memcached-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.memcached :as cache]
            [d-core.core.cache.protocol :as p]
            [d-core.core.clients.memcached.client :as mc]))

(deftest memcached-cache-lookup
  (testing "Memcached cache lookup delegates to client"
    (let [calls (atom [])
          c (cache/->MemcachedCache :mem)]
      (with-redefs [mc/get* (fn [client key]
                              (swap! calls conj {:client client :key key})
                              :hit)]
        (is (= :hit (p/cache-lookup c "k" nil)))
        (is (= [{:client :mem :key "k"}] @calls))))))

(deftest memcached-cache-put
  (testing "Memcached cache put passes ttl and returns value"
    (let [calls (atom [])
          c (cache/->MemcachedCache :mem)]
      (with-redefs [mc/set* (fn [client key ttl value]
                              (swap! calls conj {:client client :key key :ttl ttl :value value})
                              :ok)]
        (is (= "v" (p/cache-put c "k" "v" {:ttl 30})))
        (is (= [{:client :mem :key "k" :ttl 30 :value "v"}] @calls))))))

(deftest memcached-cache-delete
  (testing "Memcached cache delete delegates to client"
    (let [calls (atom [])
          c (cache/->MemcachedCache :mem)]
      (with-redefs [mc/delete* (fn [client key]
                                 (swap! calls conj {:client client :key key})
                                 :ok)]
        (is (nil? (p/cache-delete c "k" nil)))
        (is (= [{:client :mem :key "k"}] @calls))))))

(deftest memcached-cache-clear
  (testing "Memcached cache clear delegates to client"
    (let [calls (atom [])
          c (cache/->MemcachedCache :mem)]
      (with-redefs [mc/flush* (fn [client]
                                (swap! calls conj {:client client})
                                :ok)]
        (is (nil? (p/cache-clear c nil)))
        (is (= [{:client :mem}] @calls))))))
