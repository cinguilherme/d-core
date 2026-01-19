(ns d-core.core.clients.memcached.client
  (:require [clojure.string :as str])
  (:import (net.spy.memcached MemcachedClient)
           (java.net InetSocketAddress)))

(defrecord MemcachedClientHandle [^MemcachedClient client servers]
  Object
  (toString [_] (str "#MemcachedClient{:servers " (pr-str servers) "}")))

(defn- ->address
  [server]
  (cond
    (string? server)
    (let [[host port] (str/split server #":" 2)
          port (Integer/parseInt (or port "11211"))]
      (InetSocketAddress. host (int port)))

    (map? server)
    (InetSocketAddress. (or (:host server) "localhost")
                        (int (or (:port server) 11211)))

    :else
    (InetSocketAddress. "localhost" 11211)))

(defn make-client
  [{:keys [servers host port]
    :or {servers nil}}]
  (let [servers (or servers
                    [(str (or host "localhost") ":" (or port 11211))])
        addresses (mapv ->address servers)
        ^MemcachedClient client (MemcachedClient. addresses)]
    (->MemcachedClientHandle client servers)))

(defn get*
  [client key]
  (.get ^MemcachedClient (:client client) (str key)))

(defn set*
  [client key ttl value]
  (.set ^MemcachedClient (:client client) (str key) (int (or ttl 0)) value))

(defn delete*
  [client key]
  (.delete ^MemcachedClient (:client client) (str key)))

(defn flush*
  [client]
  (.flush ^MemcachedClient (:client client)))

(defn close!
  [client]
  (when-let [^MemcachedClient c (:client client)]
    (.shutdown c)))
