(ns d-core.integration.tile38-helpers
  (:require [d-core.core.clients.tile38.client :as t38]))

(defn integration-enabled?
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

(defn with-tile38-client
  "Tries each candidate Tile38 URI in order, probing with HOOKS before running
  the test body `f`. Falls back to the next URI if the probe throws or returns
  a non-ok response. Throws when no URI succeeds."
  [f]
  (let [uris (tile38-uris)
        explicit? (some? (tile38-uri))]
    (loop [remaining uris
           last-error nil]
      (if-let [uri (first remaining)]
        (let [client (t38/make-client {:uri uri})
              result (try
                       (let [probe-res (t38/hooks! client {:limit 1})]
                         (when-not (:ok probe-res)
                           (throw (ex-info "Tile38 probe failed"
                                           {:uri uri
                                            :probe-response probe-res})))
                         {:ok true :value (f client)})
                       (catch Exception e
                         {:ok false :error e}))]
          (if (:ok result)
            (:value result)
            (if (and (not explicit?) (next remaining))
              (recur (rest remaining) (:error result))
              (throw (:error result)))))
        (throw (or last-error (ex-info "No Tile38 URI configured" {})))))))
