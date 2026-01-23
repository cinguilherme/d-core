(ns d-core.integration.tile38-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.tile38.client :as t38])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_TILE38"))))

(defn- tile38-uri
  []
  (System/getenv "DCORE_TILE38_URI"))

(defn- tile38-uris
  []
  (if-let [uri (tile38-uri)]
    [uri]
    (let [host (or (System/getenv "DCORE_TILE38_HOST") "localhost")
          port (or (System/getenv "DCORE_TILE38_PORT") "9851")
          password (System/getenv "DCORE_TILE38_PASSWORD")
          default-password "tile38"]
      (->> [(when (seq password)
              (str "redis://:" password "@" host ":" port))
            (str "redis://:" default-password "@" host ":" port)
            (str "redis://" host ":" port)]
           (remove nil?)
           distinct
           vec))))

(defn- with-tile38-client
  [f]
  (let [uris (tile38-uris)
        explicit? (some? (tile38-uri))]
    (loop [remaining uris
           last-error nil]
      (if-let [uri (first remaining)]
        (let [client (t38/make-client {:uri uri})
              result (try
                       {:ok true :value (f client)}
                       (catch Exception e
                         {:ok false :error e}))]
          (if (:ok result)
            (:value result)
            (if (and (not explicit?) (next remaining))
              (recur (rest remaining) (:error result))
              (throw (:error result)))))
        (throw (or last-error (ex-info "No Tile38 URI configured" {})))))))

(deftest tile38-basic-ops
  (testing "set/get/nearby roundtrip"
    (if-not (integration-enabled?)
      (is true "Skipping Tile38 integration test; set INTEGRATION=1")
      (with-tile38-client
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
