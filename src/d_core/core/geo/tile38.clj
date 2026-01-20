(ns d-core.core.geo.tile38
  (:require [integrant.core :as ig]
            [d-core.core.clients.tile38.client :as tc]
            [d-core.core.geo.protocol :as p]))

(defrecord Tile38Index [client]
  p/GeoIndexProtocol
  (set-point! [_ key id point opts]
    (tc/set-point! client key id point opts))
  (set-object! [_ key id object opts]
    (tc/set-object! client key id object opts))
  (set-bounds! [_ key id bounds opts]
    (tc/set-bounds! client key id bounds opts))
  (get-object [_ key id opts]
    (tc/get! client key id opts))
  (delete! [_ key ids _opts]
    (let [ids (if (sequential? ids) ids [ids])]
      (apply tc/del! client key ids)))
  (drop! [_ key opts]
    (tc/drop! client key opts))
  (scan [_ key opts]
    (tc/scan! client key opts))
  (nearby [_ key point opts]
    (tc/nearby! client key point opts))
  (within [_ key shape opts]
    (tc/within! client key shape opts))
  (intersects [_ key shape opts]
    (tc/intersects! client key shape opts)))

(defmethod ig/init-key :d-core.core.geo.tile38/index
  [_ {:keys [tile38-client] :as opts}]
  (when-not tile38-client
    (throw (ex-info "Tile38 index requires :tile38-client" {:opts opts})))
  (->Tile38Index tile38-client))
