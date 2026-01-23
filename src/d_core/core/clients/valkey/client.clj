(ns d-core.core.clients.valkey.client)

(defrecord ValkeyClient [conn]
  Object
  (toString [_] (str "#ValkeyClient" (dissoc conn :spec))))

(defn make-client
  [{:keys [uri] :or {uri "redis://localhost:6379"}}]
  (->ValkeyClient {:pool {}
                   :spec {:uri uri}}))
