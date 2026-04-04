(ns d-core.core.ai.common
  (:require [d-core.core.ai.protocol :as p]
            [duct.logger :as logger]))

(defn- log*
  [maybe-logger level event data]
  (when maybe-logger
    (logger/log maybe-logger level event data)))

(defn- delegate
  [providers default-provider opts]
  (let [provider (or (:provider opts) default-provider)
        backend (get providers provider)]
    (when-not backend
      (throw (ex-info "Unknown AI provider"
                      {:provider provider
                       :known (keys providers)})))
    [provider backend]))

(defrecord CommonAI [default-provider providers logger]
  p/GenerationProtocol
  (generate [_ request opts]
    (let [[provider backend] (delegate providers default-provider opts)]
      (log* logger :debug ::generate {:provider provider})
      (p/generate backend request opts)))

  p/ModelCapabilitiesProtocol
  (capabilities [_ opts]
    (let [[provider backend] (delegate providers default-provider opts)]
      (log* logger :debug ::capabilities {:provider provider})
      (p/capabilities backend opts))))

(defn init-common
  [{:keys [default-provider providers logger]
    :or {default-provider :lm-studio-openai}}]
  (->CommonAI default-provider providers logger))
