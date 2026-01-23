(ns d-core.integration.messaging-routing-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core :as json]
            [integrant.core :as ig]
            [d-core.queue :as q]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.consumers.consumer]
            [d-core.core.messaging.routing]
            [d-core.core.messaging.dead-letter.sinks.storage]
            [d-core.core.messaging.dead-letter.admin.storage]
            [d-core.core.messaging.dead-letter.admin.protocol :as dl-admin]
            [d-core.core.producers.in-memory]
            [d-core.core.storage.local-disk]))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))))

(defn- wait-for
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 10) (recur))
          false)))))

(defn- delete-recursively
  [^java.io.File root]
  (when (and root (.exists root))
    (doseq [f (reverse (file-seq root))]
      (io/delete-file f true))))

(def default-routing
  {:defaults {:source :redis
              :deadletter {:sink :hybrid
                           :policy :default
                           :max-attempts 3
                           :delay-ms 100
                           :suffix ".dl"}}
   :topics {:default {:source :redis
                      :stream "core:default"
                      :group "core"}
            :sample-fail {:source :redis
                          :stream "sample:fail-sample"
                          :group "sample"}
            :test-queue {:source :in-memory}}
   :subscriptions {:default {:source :redis
                             :topic :default
                             :handler :log-consumed
                             :options {:block-ms 50}}
                   :sample-fail {:source :redis
                                 :topic :sample-fail
                                 :handler :log-consumed
                                 :options {:block-ms 50}
                                 :deadletter {:sink :hybrid
                                              :policy :default
                                              :max-attempts 3
                                              :delay-ms 100
                                              :suffix ".dl"}}}})

(defn- dlq-id-for-topic
  [root-path topic]
  (let [topic-name (if (keyword? topic) (name topic) (str topic))
        path (io/file root-path "dead-letters" "index" "by-topic" (str topic-name ".jsonl"))]
    (when (.exists path)
      (let [line (->> (str/split-lines (slurp path))
                      (remove str/blank?)
                      last)]
        (when line
          (:dlq-id (json/parse-string line true)))))))

(deftest integration-routing-deadletter-replay
  (testing "DLQ storage + admin replay routes message back to in-memory consumer"
    (if-not (integration-enabled?)
      (is true "Skipping integration test; set INTEGRATION=1")
      (let [calls (atom [])
            fail-once? (atom false)
            handlers {:log-consumed (fn [envelope]
                                      (swap! calls conj {:handler :log-consumed
                                                         :envelope envelope}))
                      :fail-once (fn [envelope]
                                   (if (compare-and-set! fail-once? false true)
                                     (throw (ex-info "fail-once" {:failure/type :transient}))
                                     (swap! calls conj {:handler :fail-once
                                                        :envelope envelope})))}
            overrides {:handlers handlers
                       :subscriptions {:sample-fail {:source :in-memory
                                                     :topic :test-queue
                                                     :handler :fail-once
                                                     :options {:block-ms 10}}}}
            logger (:logger (h-logger/make-test-logger))
            root-path (let [dir (java.nio.file.Files/createTempDirectory
                                 "dcore-dlq"
                                 (make-array java.nio.file.attribute.FileAttribute 0))]
                        (str dir))
            cfg {:d-core.queue/in-memory-queues {}
                 :d-core.core.producers.in-memory/producer {:queues (ig/ref :d-core.queue/in-memory-queues)
                                                            :logger logger}
                 :d-core.core.storage/local-disk {:root-path root-path
                                                  :logger logger}
                 :d-core.core.messaging.dead-letter/storage {:storage (ig/ref :d-core.core.storage/local-disk)
                                                             :logger logger}
                 :d-core.core.messaging.dead-letter.admin/storage {:storage (ig/ref :d-core.core.storage/local-disk)
                                                                   :producer (ig/ref :d-core.core.producers.in-memory/producer)
                                                                   :logger logger}
                 :d-core.core.messaging/routing {:default-routing default-routing
                                                 :overrides overrides}
                 :d-core.core.consumers.consumer/consumer {:queues (ig/ref :d-core.queue/in-memory-queues)
                                                           :routing (ig/ref :d-core.core.messaging/routing)
                                                           :dead-letter (ig/ref :d-core.core.messaging.dead-letter/storage)
                                                           :default-poll-ms 5
                                                           :logger logger}}
            system (ig/init cfg)]
        (try
          (let [queues (:d-core.queue/in-memory-queues system)
                admin (:d-core.core.messaging.dead-letter.admin/storage system)
                fail-queue (q/get-queue! queues :test-queue)
                payload {:msg {:id 1}}]
            (q/enqueue! fail-queue payload)
            (is (wait-for #(some? (dlq-id-for-topic root-path :test-queue)) 1500))
            (let [dlq-id (dlq-id-for-topic root-path :test-queue)]
              (is (some? dlq-id))
              (dl-admin/replay-deadletter! admin dlq-id {:topic :test-queue
                                                        :producer :in-memory}))
            (is (wait-for #(some (fn [call] (= :fail-once (:handler call))) @calls) 1500)))
          (finally
            (ig/halt! system)
            (delete-recursively (io/file root-path))))))))
