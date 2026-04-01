(ns d-core.core.geocoding.cached-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.protocol :as cache]
            [d-core.core.geocoding.cached :as sut]
            [d-core.core.geocoding.protocol :as p]))

(defrecord StubCache [store puts]
  cache/CacheProtocol
  (cache-lookup [_ key _opts]
    (get @store key))
  (cache-put [_ key value opts]
    (swap! store assoc key value)
    (swap! puts conj {:key key :opts opts :value value})
    value)
  (cache-delete [_ key _opts]
    (swap! store dissoc key)
    nil)
  (cache-clear [_ _opts]
    (reset! store {})
    nil))

(defrecord StubGeocoder [calls geocode-response reverse-response geocode-error reverse-error]
  p/GeocodingProtocol
  (geocode [_ query _opts]
    (swap! calls conj [:geocode query])
    (if geocode-error
      (throw geocode-error)
      geocode-response))
  (reverse-geocode [_ location _opts]
    (swap! calls conj [:reverse-geocode location])
    (if reverse-error
      (throw reverse-error)
      reverse-response)))

(defn- make-cache
  []
  (map->StubCache {:store (atom {})
                   :puts (atom [])}))

(defn- make-wrapper
  [{:keys [geocode-response reverse-response geocode-error reverse-error]}]
  (let [calls (atom [])
        cache (make-cache)
        geocoder (map->StubGeocoder {:calls calls
                                     :geocode-response geocode-response
                                     :reverse-response reverse-response
                                     :geocode-error geocode-error
                                     :reverse-error reverse-error})]
    {:calls calls
     :cache cache
     :wrapper (sut/->CachedGeocoder :nominatim geocoder cache (* 30 24 60 60 1000) (* 60 60 1000))}))

(deftest geocode-cache-hit
  (testing "repeated geocode requests hit the cache after the first miss"
    (let [{:keys [calls cache wrapper]} (make-wrapper {:geocode-response {:items [{:location {:lat 43.7 :lon 7.4}
                                                                                   :components {}
                                                                                   :provider {:name :nominatim}}]
                                                                          :provider :nominatim}})]
      (is (= :nominatim (:provider (p/geocode wrapper {:text "Monaco"} {}))))
      (is (= :nominatim (:provider (p/geocode wrapper {:text "Monaco"} {}))))
      (is (= [[:geocode {:text "Monaco"}]] @calls))
      (is (= 1 (count @(:puts cache))))
      (is (= (* 30 24 60 60 1000) (get-in @(:puts cache) [0 :opts :ttl-ms]))))))

(deftest reverse-cache-hit
  (testing "repeated reverse-geocode requests hit the cache after the first miss"
    (let [{:keys [calls cache wrapper]} (make-wrapper {:reverse-response {:items [{:location {:lat 43.7334 :lon 7.4221}
                                                                                   :components {}
                                                                                   :provider {:name :nominatim}}]
                                                                          :provider :nominatim}})]
      (is (= :nominatim (:provider (p/reverse-geocode wrapper {:lat 43.7334 :lon 7.4221} {}))))
      (is (= :nominatim (:provider (p/reverse-geocode wrapper {:lat 43.7334 :lon 7.4221} {}))))
      (is (= [[:reverse-geocode {:lat 43.7334 :lon 7.4221}]] @calls))
      (is (= 1 (count @(:puts cache)))))))

(deftest operations-produce-distinct-cache-keys
  (testing "forward and reverse operations do not collide in the cache"
    (let [{:keys [cache wrapper]} (make-wrapper {:geocode-response {:items [{:location {:lat 43.7 :lon 7.4}
                                                                             :components {}
                                                                             :provider {:name :nominatim}}]
                                                                    :provider :nominatim}
                                                 :reverse-response {:items [{:location {:lat 43.7334 :lon 7.4221}
                                                                             :components {}
                                                                             :provider {:name :nominatim}}]
                                                                    :provider :nominatim}})]
      (p/geocode wrapper {:text "Monaco"} {})
      (p/reverse-geocode wrapper {:lat 43.7334 :lon 7.4221} {})
      (is (= 2 (count @(:store cache))))
      (is (apply distinct? (keys @(:store cache)))))))

(deftest empty-results-use-the-shorter-ttl
  (testing "empty result envelopes use the empty-result TTL"
    (let [{:keys [cache wrapper]} (make-wrapper {:geocode-response {:items []
                                                                    :provider :nominatim}})]
      (is (= [] (:items (p/geocode wrapper {:text "notarealplacezzzzzzzz"} {}))))
      (is (= (* 60 60 1000) (get-in @(:puts cache) [0 :opts :ttl-ms])))
      (is (= 3600 (get-in @(:puts cache) [0 :opts :ttl]))))))

(deftest exceptions-are-not-cached
  (testing "provider exceptions do not populate the cache"
    (let [{:keys [cache wrapper]} (make-wrapper {:geocode-error (ex-info "boom" {})})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (p/geocode wrapper {:text "Monaco"} {})))
      (is (empty? @(:store cache)))
      (is (empty? @(:puts cache))))))
