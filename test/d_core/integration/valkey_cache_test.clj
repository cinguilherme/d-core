(ns d-core.integration.valkey-cache-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.cache.protocol :as p]
            [d-core.core.cache.valkey :as valkey]
            [d-core.core.clients.valkey.client :as vc])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_VALKEY"))))

(defn- valkey-uri
  []
  (or (System/getenv "DCORE_VALKEY_URI")
      "redis://localhost:6380"))

(deftest valkey-cache-basic-ops
  (testing "put/get/delete/clear roundtrip"
    (if-not (integration-enabled?)
      (is true "Skipping Valkey integration test; set DCORE_INTEGRATION=1")
      (let [client (vc/make-client {:uri (valkey-uri)})
            cache (valkey/->ValkeyCache client)
            base-key (str "dcore.int.valkey." (UUID/randomUUID))
            k1 (str base-key ":k1")
            k2 (str base-key ":k2")]
        (try
          (p/cache-put cache k1 "v1" nil)
          (is (= "v1" (p/cache-lookup cache k1 nil)))

          (p/cache-delete cache k1 nil)
          (is (nil? (p/cache-lookup cache k1 nil)))

          (p/cache-put cache k1 "v1" {:ttl 1})
          (Thread/sleep 1200)
          (is (nil? (p/cache-lookup cache k1 nil)))

          (p/cache-put cache k1 "v1" nil)
          (p/cache-put cache k2 "v2" nil)
          (p/cache-clear cache nil)
          (is (nil? (p/cache-lookup cache k1 nil)))
          (is (nil? (p/cache-lookup cache k2 nil)))
          (finally
            (p/cache-clear cache nil)))))))
