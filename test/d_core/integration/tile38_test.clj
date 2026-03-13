(ns d-core.integration.tile38-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.tile38.client :as t38]
            [d-core.integration.tile38-helpers :as th])
  (:import (java.util UUID)))

(deftest tile38-basic-ops
  (testing "set/get/nearby roundtrip"
    (if-not (th/integration-enabled?)
      (is true "Skipping Tile38 integration test; set INTEGRATION=1")
      (th/with-tile38-client
        (fn [client]
          (let [key (str "dcore.int.geo." (UUID/randomUUID))
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
                (try
                  (t38/drop! client key {})
                  (catch Exception _e
                    nil))))))))))
