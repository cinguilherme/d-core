(ns d-core.core.cache.common-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.common :as common]
            [d-core.core.cache.protocol :as p]
            [d-core.helpers.logger :as h-logger]))

(defn- make-cache
  [cache-id calls]
  (reify p/CacheProtocol
    (cache-lookup [_ key opts]
      (swap! calls conj {:cache cache-id :op :lookup :key key :opts opts})
      :lookup)
    (cache-put [_ key value opts]
      (swap! calls conj {:cache cache-id :op :put :key key :value value :opts opts})
      :put)
    (cache-delete [_ key opts]
      (swap! calls conj {:cache cache-id :op :delete :key key :opts opts})
      :delete)
    (cache-clear [_ opts]
      (swap! calls conj {:cache cache-id :op :clear :opts opts})
      :clear)))

(deftest common-cache-delegation
  (testing "Common cache delegates to default or selected cache"
    (let [calls (atom [])
          logger (:logger (h-logger/make-test-logger))
          cache-a (make-cache :a calls)
          cache-b (make-cache :b calls)
          c (common/->CommonCache :a {:a cache-a :b cache-b} logger)]
      (is (= :lookup (p/cache-lookup c :k nil)))
      (is (= :put (p/cache-put c :k :v {:cache :b})))
      (is (= :delete (p/cache-delete c :k {:cache :b})))
      (is (= :clear (p/cache-clear c {:cache :a})))
      (is (= [{:cache :a :op :lookup :key :k :opts nil}
              {:cache :b :op :put :key :k :value :v :opts {:cache :b}}
              {:cache :b :op :delete :key :k :opts {:cache :b}}
              {:cache :a :op :clear :opts {:cache :a}}]
             @calls)))))
