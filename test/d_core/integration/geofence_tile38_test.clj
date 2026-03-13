(ns d-core.integration.geofence-tile38-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.tile38.client :as t38]
            [d-core.core.geofence.protocol :as gp]
            [d-core.core.geofence.tile38 :as geo]
            [d-core.integration.tile38-helpers :as th])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetSocketAddress)
           (java.nio.charset StandardCharsets)
           (java.util UUID)))

(defn- hook-host
  []
  (or (System/getenv "DCORE_TILE38_HOOK_HOST")
      "host.docker.internal"))

(defn- start-capture-server!
  []
  (let [requests (atom [])
        server (HttpServer/create (InetSocketAddress. "0.0.0.0" 0) 0)
        path "/hook"]
    (.createContext server path
                    (reify HttpHandler
                      (^void handle [_ ^HttpExchange exchange]
                        (let [body (slurp (.getRequestBody exchange) :encoding "UTF-8")
                              payload (if (seq body)
                                        (json/parse-string body true)
                                        nil)
                              bytes (.getBytes "ok" StandardCharsets/UTF_8)]
                          (when payload
                            (swap! requests conj payload))
                          (.sendResponseHeaders exchange 200 (alength bytes))
                          (with-open [os (.getResponseBody exchange)]
                            (.write os bytes))
                          nil))))
    (.setExecutor server nil)
    (.start server)
    {:server server
     :requests requests
     :url (str "http://" (hook-host) ":" (.getPort (.getAddress server)) path)}))

(defn- stop-capture-server!
  [{:keys [^HttpServer server]}]
  (when server
    (.stop server 0)))

(defn- cleanup-test-artifacts!
  [client hook-id key capture]
  (try (t38/delhook! client hook-id {}) (catch Exception _e nil))
  (try (t38/drop! client key {}) (catch Exception _e nil))
  (stop-capture-server! capture))

(defn- await-predicate
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do
                (Thread/sleep 100)
                (recur))))))

(deftest tile38-static-geofence-hook
  (testing "static nearby hook emits enter and exit"
    (if-not (th/integration-enabled?)
      (is true "Skipping Tile38 geofence integration test; set INTEGRATION=1")
      (th/with-tile38-client
        (fn [client]
          (let [capture (start-capture-server!)
                geofence (geo/->Tile38Geofence client)
                key (str "dcore.int.geofence." (UUID/randomUUID))
                hook-id (str "airport-zone-" (UUID/randomUUID))
                driver-id "driver:123"
                outside {:lat 33.70 :lon -112.26}
                inside {:lat 33.5123 :lon -112.2693}]
            (try
              (is (= true
                     (:ok (gp/upsert-fence! geofence
                                            {:id hook-id
                                             :kind :static
                                             :source {:key key}
                                             :shape {:kind :circle
                                                     :center inside
                                                     :radius-m 1500}
                                             :operation :nearby
                                             :delivery {:endpoint (:url capture)}
                                             :detect #{:enter :exit}
                                             :commands #{:set}}
                                            {}))))
              (Thread/sleep 250)
              (is (= true (:ok (t38/set-point! client key driver-id outside {}))))
              (Thread/sleep 250)
              (reset! (:requests capture) [])
              (is (= true (:ok (t38/set-point! client key driver-id inside {}))))
              (is (= true (await-predicate #(seq @(:requests capture)) 5000)))
              (is (= true (:ok (t38/set-point! client key driver-id outside {}))))
              (is (= true (await-predicate #(>= (count @(:requests capture)) 2) 5000)))
              (let [detects (mapv :detect @(:requests capture))]
                (is (= [hook-id hook-id] (mapv :hook @(:requests capture))))
                (is (= driver-id (:id (first @(:requests capture)))))
                (is (= #{"enter" "exit"} (set detects))))
              (finally
                (cleanup-test-artifacts! client hook-id key capture))))))))

(deftest tile38-roam-hook-with-match
  (testing "roaming hook accepts MATCH + ROAM and emits roam payloads"
    (if-not (th/integration-enabled?)
      (is true "Skipping Tile38 geofence integration test; set INTEGRATION=1")
      (th/with-tile38-client
        (fn [client]
          (let [capture (start-capture-server!)
                geofence (geo/->Tile38Geofence client)
                key (str "dcore.int.geofence." (UUID/randomUUID))
                hook-id (str "dispatch-candidate-" (UUID/randomUUID))
                passenger-id "passenger:999"
                driver-id "driver:123"
                passenger-point {:lat 33.5123 :lon -112.2693}
                far-point {:lat 33.70 :lon -112.26}
                near-point {:lat 33.5125 :lon -112.2695}]
            (try
              (is (= true
                     (:ok (gp/upsert-fence! geofence
                                            {:id hook-id
                                             :kind :roam
                                             :source {:key key
                                                      :match "driver:*"}
                                             :target {:key key
                                                      :pattern "passenger:*"
                                                      :radius-m 1500}
                                             :delivery {:endpoint (:url capture)}
                                             :detect #{:roam}
                                             :commands #{:set}
                                             :no-dwell? true}
                                            {}))))
              (Thread/sleep 250)
              (is (= true (:ok (t38/set-point! client key passenger-id passenger-point {}))))
              (is (= true (:ok (t38/set-point! client key driver-id far-point {}))))
              (Thread/sleep 250)
              (reset! (:requests capture) [])
              (is (= true (:ok (t38/set-point! client key driver-id near-point {}))))
              (is (= true (await-predicate #(seq @(:requests capture)) 5000)))
              (let [event (first @(:requests capture))]
                (is (= hook-id (:hook event)))
                (is (= "roam" (:detect event)))
                (is (= driver-id (:id event)))
                (is (= passenger-id (get-in event [:nearby :id]))))
              (finally
                (cleanup-test-artifacts! client hook-id key capture))))))))
