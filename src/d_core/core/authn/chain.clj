(ns d-core.core.authn.chain
  (:require [duct.logger :as logger]
            [integrant.core :as ig]
            [d-core.core.authn.protocol :as p]))

(def ^:private default-continue-on
  #{:d-core.core.authn.jwt/missing-token
    :d-core.core.authn.api-key/missing-api-key})

(defn- normalize-entry
  [entry]
  (if (map? entry)
    {:authenticator (:authenticator entry)
     :continue-on (set (or (:continue-on entry) default-continue-on))}
    {:authenticator entry
     :continue-on default-continue-on}))

(defrecord ChainAuthenticator [entries logger]
  p/Authenticator
  (authenticate [_ request opts]
    (loop [pending (seq entries)
           last-ex nil]
      (if (empty? pending)
        (if last-ex
          (throw last-ex)
          (throw (ex-info "No authenticator accepted request"
                          {:type ::no-authenticator-accepted})))
        (let [{:keys [authenticator continue-on]} (first pending)
              result (try
                       {:ok (p/authenticate authenticator request opts)}
                       (catch clojure.lang.ExceptionInfo ex
                         {:error ex}))]
          (if-let [ok (:ok result)]
            ok
            (let [ex (:error result)]
              (if (contains? continue-on (:type (ex-data ex)))
                (recur (next pending) ex)
                (throw ex))))))))

  (verify-token [_ token opts]
    (if-let [{:keys [authenticator]} (first entries)]
      (p/verify-token authenticator token opts)
      (throw (ex-info "No authenticators configured"
                      {:type ::no-authenticators}))))

  (challenge [_ request opts]
    (if-let [{:keys [authenticator]} (first entries)]
      (p/challenge authenticator request opts)
      {:status 401
       :body "Unauthorized"})))

(defmethod ig/init-key :d-core.core.authn.chain/authenticator
  [_ {:keys [authenticators logger]}]
  (let [entries (mapv normalize-entry authenticators)]
    (when (empty? entries)
      (throw (ex-info "Authenticator chain requires non-empty :authenticators"
                      {:type ::missing-authenticators})))
    (when logger
      (logger/log logger :info ::chain-authenticator-initialized {:count (count entries)}))
    (->ChainAuthenticator entries logger)))
