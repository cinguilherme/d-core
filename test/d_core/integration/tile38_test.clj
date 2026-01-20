(ns d-core.integration.tile38-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.tile38.client :as t38])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_TILE38"))))

(defn- tile38-uri
  []
  (or (System/getenv "DCORE_TILE38_URI")
      "redis://:tile38@localhost:9851"))

(deftest tile38-basic-ops
  (testing "set/get/nearby roundtrip"
    (if-not (integration-enabled?)
      (is true "Skipping Tile38 integration test; set DCORE_INTEGRATION=1")
      (let [client (t38/make-client {:uri (tile38-uri)})
            key (str "dcore.int.geo." (UUID/randomUUID))
            id "truck-1"
            point {:lat 33.5123 :lon -112.2693}]
        (try
          (let [set-res (t38/set-point! client key id point {})
                get-res (t38/get! client key id {:type :point})
                near-res (t38/nearby! client key point {:radius 5000 :limit 10})
                ids (set (map :id (:objects near-res)))]
            (is (= true (:ok set-res)))
            (is (= true (:ok get-res)))
            (is (= true (:ok near-res)))
            (is (contains? ids id)))
          (finally
            (t38/drop! client key {})))))))
