(ns d-core.core.cache.valkey-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.protocol :as p]
            [d-core.core.cache.valkey :as valkey]))

(deftest valkey-cache-lookup
  (testing "Valkey cache lookup delegates to valkey-get"
    (let [calls (atom [])
          c (valkey/->ValkeyCache :valkey)]
      (with-redefs [d-core.core.cache.valkey/valkey-get (fn [client key]
                                                          (swap! calls conj {:client client :key key})
                                                          :hit)]
        (is (= :hit (p/cache-lookup c "k" nil)))
        (is (= [{:client :valkey :key "k"}] @calls))))))

(deftest valkey-cache-put
  (testing "Valkey cache put uses setex when ttl is provided"
    (let [calls (atom [])
          c (valkey/->ValkeyCache :valkey)]
      (with-redefs [d-core.core.cache.valkey/valkey-set (fn [client key value]
                                                          (swap! calls conj {:op :set :client client :key key :value value})
                                                          :ok)
                    d-core.core.cache.valkey/valkey-setex (fn [client key ttl value]
                                                            (swap! calls conj {:op :setex :client client :key key :ttl ttl :value value})
                                                            :ok-ttl)]
        (is (= :ok (p/cache-put c "k1" "v1" {})))
        (is (= :ok-ttl (p/cache-put c "k2" "v2" {:ttl 10})))
        (is (= [{:op :set :client :valkey :key "k1" :value "v1"}
                {:op :setex :client :valkey :key "k2" :ttl 10 :value "v2"}]
               @calls))))))

(deftest valkey-cache-delete
  (testing "Valkey cache delete delegates to valkey-del"
    (let [calls (atom [])
          c (valkey/->ValkeyCache :valkey)]
      (with-redefs [d-core.core.cache.valkey/valkey-del (fn [client key]
                                                          (swap! calls conj {:client client :key key})
                                                          :gone)]
        (is (= :gone (p/cache-delete c "k" nil)))
        (is (= [{:client :valkey :key "k"}] @calls))))))

(deftest valkey-cache-clear
  (testing "Valkey cache clear delegates to valkey-flushdb"
    (let [calls (atom [])
          c (valkey/->ValkeyCache :valkey)]
      (with-redefs [d-core.core.cache.valkey/valkey-flushdb (fn [client]
                                                              (swap! calls conj {:client client})
                                                              :ok)]
        (is (= :ok (p/cache-clear c nil)))
        (is (= [{:client :valkey}] @calls))))))
