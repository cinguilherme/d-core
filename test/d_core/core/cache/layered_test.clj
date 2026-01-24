(ns d-core.core.cache.layered-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.layered :as layered]
            [d-core.core.cache.protocol :as p]
            [d-core.core.cache.in-memory :as in-memory]
            [d-core.core.codecs.bytes :as bytes-codec]
            [d-core.core.compression.gzip :as gzip]
            [d-core.helpers.logger :as h-logger]))

(defn- make-cache
  [cache-id calls handlers]
  (reify p/CacheProtocol
    (cache-lookup [_ key opts]
      (swap! calls conj {:cache cache-id :op :lookup :key key :opts opts})
      (if-let [f (:lookup handlers)] (f key opts) nil))
    (cache-put [_ key value opts]
      (swap! calls conj {:cache cache-id :op :put :key key :value value :opts opts})
      (if-let [f (:put handlers)] (f key value opts) value))
    (cache-delete [_ key opts]
      (swap! calls conj {:cache cache-id :op :delete :key key :opts opts})
      (if-let [f (:delete handlers)] (f key opts) nil))
    (cache-clear [_ opts]
      (swap! calls conj {:cache cache-id :op :clear :opts opts})
      (if-let [f (:clear handlers)] (f opts) nil))))

(deftest layered-cache-lookup-promotes
  (testing "Layered cache promotes hits to earlier tiers"
    (let [calls (atom [])
          cache-a (make-cache :a calls {:lookup (fn [_ _] nil)})
          cache-b (make-cache :b calls {:lookup (fn [_ _] :hit)})
          logger (:logger (h-logger/make-test-logger))
          c (layered/->LayeredCache [{:id :a :cache cache-a :ttl-ms 1500 :promote? true}
                                     {:id :b :cache cache-b}]
                                    nil
                                    :write-through
                                    ::layered/miss
                                    logger)]
      (is (= :hit (p/cache-lookup c :k nil)))
      (is (= [{:cache :a :op :lookup :key :k :opts {}}
              {:cache :b :op :lookup :key :k :opts {}}
              {:cache :a :op :put :key :k :value :hit :opts {:ttl 2}}]
             @calls)))))

(deftest layered-cache-lookup-read-through
  (testing "Layered cache reads through source and warms tiers"
    (let [calls (atom [])
          cache-a (make-cache :a calls {:lookup (fn [_ _] nil)})
          cache-b (make-cache :b calls {:lookup (fn [_ _] nil)})
          source {:read-fn (fn [key opts]
                             (swap! calls conj {:op :source-read :key key :opts opts})
                             :value)}
          c (layered/->LayeredCache [{:id :a :cache cache-a}
                                     {:id :b :cache cache-b}]
                                    source
                                    :write-through
                                    ::layered/miss
                                    nil)]
      (is (= :value (p/cache-lookup c :k nil)))
      (is (= [{:cache :a :op :lookup :key :k :opts {}}
              {:cache :b :op :lookup :key :k :opts {}}
              {:op :source-read :key :k :opts {}}
              {:cache :a :op :put :key :k :value :value :opts {}}
              {:cache :b :op :put :key :k :value :value :opts {}}]
             @calls)))))

(deftest layered-cache-put-write-through
  (testing "Layered cache writes through to source and tiers"
    (let [calls (atom [])
          cache-a (make-cache :a calls {})
          source {:write-fn (fn [key value opts]
                              (swap! calls conj {:op :source-write :key key :value value :opts opts})
                              :ok)}
          c (layered/->LayeredCache [{:id :a :cache cache-a :ttl-ms 2500}]
                                    source
                                    :write-through
                                    ::layered/miss
                                    nil)]
      (is (= :value (p/cache-put c :k :value nil)))
      (is (= [{:op :source-write :key :k :value :value :opts {}}
              {:cache :a :op :put :key :k :value :value :opts {:ttl 3}}]
             @calls)))))

(deftest layered-cache-chunking-roundtrip
  (testing "Layered cache chunks large byte payloads per tier"
    (let [data (byte-array (map byte (range 64)))
          mem (in-memory/->InMemoryCache (atom {}) nil)
          codec (bytes-codec/->BytesCodec)
          c (layered/->LayeredCache [{:id :mem
                                      :cache mem
                                      :codec codec
                                      :max-value-bytes 16
                                      :chunk-bytes 16}]
                                    nil
                                    :write-through
                                    ::layered/miss
                                    nil)]
      (p/cache-put c :blob data nil)
      (is (= (seq data) (seq (p/cache-lookup c :blob nil)))))))

(deftest layered-cache-compression-roundtrip
  (testing "Layered cache compresses and decompresses payloads"
    (let [data (byte-array 128)
          _ (.nextBytes (java.util.Random. 42) data)
          mem (in-memory/->InMemoryCache (atom {}) nil)
          codec (bytes-codec/->BytesCodec)
          compressor (gzip/->GzipCompression)
          c (layered/->LayeredCache [{:id :mem
                                      :cache mem
                                      :codec codec
                                      :compressor compressor}]
                                    nil
                                    :write-through
                                    ::layered/miss
                                    nil)]
      (p/cache-put c :blob data nil)
      (is (= (seq data) (seq (p/cache-lookup c :blob nil)))))))
