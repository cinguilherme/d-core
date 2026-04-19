(ns d-core.core.clients.zookeeper.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.zookeeper.client :as client])
  (:import [org.apache.curator.framework.state ConnectionState ConnectionStateListener]))

(deftest make-client-starts-and-tracks-connection-state
  (let [calls (atom [])
        listener* (atom nil)
        closed (atom nil)]
    (with-redefs [client/build-framework (fn [opts]
                                           (swap! calls conj [:build opts])
                                           :curator)
                  client/add-connection-state-listener! (fn [curator listener]
                                                          (swap! calls conj [:listen curator])
                                                          (reset! listener* listener)
                                                          curator)
                  client/start-framework! (fn [curator]
                                            (swap! calls conj [:start curator])
                                            curator)
                  client/block-until-connected (fn [curator timeout-ms]
                                                (swap! calls conj [:block curator timeout-ms])
                                                true)
                  client/close-framework! (fn [curator]
                                            (reset! closed curator))]
      (let [zk (client/make-client {:connect-string "localhost:2181"
                                    :session-timeout-ms 12000
                                    :connection-timeout-ms 2000
                                    :retry-base-sleep-ms 100
                                    :retry-max-retries 5
                                    :block-until-connected-ms 1500})
            ^ConnectionStateListener listener @listener*]
        (is (= "localhost:2181" (:connect-string zk)))
        (is (= 12000 (:session-timeout-ms zk)))
        (is (= :connected (client/current-state zk)))
        (.stateChanged listener nil ConnectionState/SUSPENDED)
        (is (= :suspended (client/current-state zk)))
        (.stateChanged listener nil ConnectionState/RECONNECTED)
        (is (= :reconnected (client/current-state zk)))
        (client/close! zk)
        (is (= :curator @closed))
        (is (= :closed (client/current-state zk))))
      (is (= [[:build {:connect-string "localhost:2181"
                       :session-timeout-ms 12000
                       :connection-timeout-ms 2000
                       :retry-base-sleep-ms 100
                       :retry-max-retries 5}]
              [:listen :curator]
              [:start :curator]
              [:block :curator 1500]]
             @calls)))))

(deftest make-client-fails-fast-on-connect-timeout
  (let [closed (atom nil)]
    (with-redefs [client/build-framework (fn [_] :curator)
                  client/add-connection-state-listener! (fn [_ _] nil)
                  client/start-framework! (fn [_] nil)
                  client/block-until-connected (fn [_ _] false)
                  client/close-framework! (fn [curator]
                                            (reset! closed curator))]
      (let [error (try
                    (client/make-client {:connect-string "localhost:2181"})
                    nil
                    (catch clojure.lang.ExceptionInfo ex
                      ex))]
        (is (instance? clojure.lang.ExceptionInfo error))
        (is (= ::client/connection-timeout (:type (ex-data error))))
        (is (= :curator @closed))))))

(deftest make-client-validates-config
  (testing "connect string is required"
    (let [error (try
                  (client/make-client {})
                  nil
                  (catch clojure.lang.ExceptionInfo ex
                    ex))]
      (is (= ::client/missing-connect-string (:type (ex-data error))))))

  (testing "timeouts must be positive integers"
    (doseq [bad ["100" 0 -1 1.5]]
      (let [error (try
                    (client/make-client {:connect-string "localhost:2181"
                                         :session-timeout-ms bad})
                    nil
                    (catch clojure.lang.ExceptionInfo ex
                      ex))]
        (is (= ::client/invalid-field (:type (ex-data error))))
        (is (= :session-timeout-ms (:field (ex-data error))))))))
