(ns d-core.core.clients.zookeeper.client
  (:require [clojure.string :as str])
  (:import [java.util.concurrent TimeUnit]
           [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory]
           [org.apache.curator.framework.state ConnectionState ConnectionStateListener]
           [org.apache.curator.retry ExponentialBackoffRetry]))

(def default-session-timeout-ms
  15000)

(def default-connection-timeout-ms
  5000)

(def default-retry-base-sleep-ms
  250)

(def default-retry-max-retries
  10)

(def default-block-until-connected-ms
  5000)

(defrecord ZooKeeperClient [curator
                            connect-string
                            session-timeout-ms
                            connection-timeout-ms
                            retry-base-sleep-ms
                            retry-max-retries
                            block-until-connected-ms
                            connection-state]
  Object
  (toString [_]
    (str "#ZooKeeperClient{:connect-string " (pr-str connect-string)
         ", :session-timeout-ms " session-timeout-ms
         ", :connection-state " (pr-str (when connection-state @connection-state)) "}")))

(defn- require-non-blank
  [value message data]
  (let [value (some-> value str str/trim)]
    (when (str/blank? value)
      (throw (ex-info message data)))
    value))

(defn- positive-long
  [value field default-value]
  (let [raw (or value default-value)]
    (when-not (and (integer? raw) (instance? Number raw))
      (throw (ex-info "ZooKeeper client field must be a positive integer"
                      {:type ::invalid-field
                       :field field
                       :value value})))
    (let [v (long raw)]
      (when (<= v 0)
        (throw (ex-info "ZooKeeper client field must be greater than zero"
                        {:type ::invalid-field
                         :field field
                         :value value})))
      v)))

(defn connection-state-keyword
  [state]
  (cond
    (nil? state) nil
    (keyword? state) state
    (instance? ConnectionState state)
    (-> ^ConnectionState state .name str/lower-case keyword)
    :else
    (-> state str str/lower-case keyword)))

(defn current-state
  [client]
  (some-> (:connection-state client) deref))

(defn connected?
  [client]
  (contains? #{:connected :reconnected} (current-state client)))

(defn safe-state?
  [client]
  (connected? client))

(defn build-framework
  [{:keys [connect-string session-timeout-ms connection-timeout-ms retry-base-sleep-ms retry-max-retries]}]
  (-> (CuratorFrameworkFactory/builder)
      (.connectString connect-string)
      (.sessionTimeoutMs (int session-timeout-ms))
      (.connectionTimeoutMs (int connection-timeout-ms))
      (.retryPolicy (ExponentialBackoffRetry. (int retry-base-sleep-ms)
                                              (int retry-max-retries)))
      (.build)))

(defn add-connection-state-listener!
  [^CuratorFramework curator listener]
  (.. curator getConnectionStateListenable (addListener ^ConnectionStateListener listener))
  curator)

(defn start-framework!
  [^CuratorFramework curator]
  (.start curator)
  curator)

(defn block-until-connected
  [^CuratorFramework curator timeout-ms]
  (.blockUntilConnected curator (long timeout-ms) TimeUnit/MILLISECONDS))

(defn close-framework!
  [^CuratorFramework curator]
  (when curator
    (.close curator)))

(defn make-client
  [{:keys [connect-string session-timeout-ms connection-timeout-ms retry-base-sleep-ms retry-max-retries block-until-connected-ms]}]
  (let [connect-string (require-non-blank connect-string
                                          "ZooKeeper client requires a non-blank :connect-string"
                                          {:type ::missing-connect-string})
        session-timeout-ms (positive-long session-timeout-ms :session-timeout-ms default-session-timeout-ms)
        connection-timeout-ms (positive-long connection-timeout-ms :connection-timeout-ms default-connection-timeout-ms)
        retry-base-sleep-ms (positive-long retry-base-sleep-ms :retry-base-sleep-ms default-retry-base-sleep-ms)
        retry-max-retries (positive-long retry-max-retries :retry-max-retries default-retry-max-retries)
        block-until-connected-ms (positive-long block-until-connected-ms :block-until-connected-ms default-block-until-connected-ms)
        connection-state (atom :connecting)
        curator (build-framework {:connect-string connect-string
                                  :session-timeout-ms session-timeout-ms
                                  :connection-timeout-ms connection-timeout-ms
                                  :retry-base-sleep-ms retry-base-sleep-ms
                                  :retry-max-retries retry-max-retries})
        listener (reify ConnectionStateListener
                   (stateChanged [_ _ new-state]
                     (reset! connection-state (connection-state-keyword new-state))))]
    (try
      (add-connection-state-listener! curator listener)
      (start-framework! curator)
      (when-not (block-until-connected curator block-until-connected-ms)
        (throw (ex-info "ZooKeeper client failed to connect before timeout"
                        {:type ::connection-timeout
                         :connect-string connect-string
                         :block-until-connected-ms block-until-connected-ms})))
      (when-not (connected? {:connection-state connection-state})
        (reset! connection-state :connected))
      (->ZooKeeperClient curator
                         connect-string
                         session-timeout-ms
                         connection-timeout-ms
                         retry-base-sleep-ms
                         retry-max-retries
                         block-until-connected-ms
                         connection-state)
      (catch Exception ex
        (close-framework! curator)
        (throw ex)))))

(defn close!
  [client]
  (close-framework! (:curator client))
  (when-let [state (:connection-state client)]
    (reset! state :closed))
  nil)
