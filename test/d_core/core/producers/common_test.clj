(ns d-core.core.producers.common-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.producers.common :as common-producer]
            [d-core.core.producers.protocol :as p]))

(defn- recording-producer
  [id calls]
  (reify p/Producer
    (produce! [_ msg options]
      (swap! calls conj {:id id :msg msg :options options})
      {:ok true :producer id})))

(defn- failing-producer
  [id calls]
  (reify p/Producer
    (produce! [_ _ _]
      (swap! calls conj {:id id})
      (throw (ex-info "boom" {:producer id})))))

(deftest common-producer-fanout-sources
  (testing "produces to all routing :sources in order"
    (let [calls (atom [])
          producers {:redis (recording-producer :redis calls)
                     :kafka (recording-producer :kafka calls)}
          routing {:topics {:orders {:sources [:redis :kafka]}}
                   :defaults {:source :in-memory}}
          producer (common-producer/->CommonProducer :in-memory producers routing nil)
          res (p/produce! producer {:id 1} {:topic :orders})]
      (is (= [:redis :kafka] (mapv :id @calls)))
      (is (= [:redis :kafka] (mapv #(get-in % [:options :source]) @calls)))
      (is (= [{:ok true :producer :redis}
              {:ok true :producer :kafka}]
             (mapv #(select-keys % [:ok :producer]) res))))))

(deftest common-producer-explicit-producer-overrides-routing
  (testing "explicit :producer overrides routing :sources"
    (let [calls (atom [])
          producers {:redis (recording-producer :redis calls)
                     :kafka (recording-producer :kafka calls)}
          routing {:topics {:orders {:sources [:redis :kafka]}}}
          producer (common-producer/->CommonProducer :in-memory producers routing nil)
          res (p/produce! producer {:id 1} {:topic :orders :producer :kafka})]
      (is (= [:kafka] (mapv :id @calls)))
      (is (= {:ok true :producer :kafka}
             (select-keys res [:ok :producer]))))))

(deftest common-producer-options-sources
  (testing "explicit :sources in options overrides routing"
    (let [calls (atom [])
          producers {:redis (recording-producer :redis calls)
                     :kafka (recording-producer :kafka calls)}
          routing {:topics {:orders {:sources [:redis :kafka]}}}
          producer (common-producer/->CommonProducer :in-memory producers routing nil)
          res (p/produce! producer {:id 1} {:topic :orders :sources [:kafka :redis]})]
      (is (= [:kafka :redis] (mapv :id @calls)))
      (is (= [:kafka :redis] (mapv #(get-in % [:options :source]) @calls)))
      (is (= [{:ok true :producer :kafka}
              {:ok true :producer :redis}]
             (mapv #(select-keys % [:ok :producer]) res))))))

(deftest common-producer-fanout-fails-fast
  (testing "fanout stops on first failure"
    (let [calls (atom [])
          producers {:a (recording-producer :a calls)
                     :b (failing-producer :b calls)
                     :c (recording-producer :c calls)}
          routing {:topics {:orders {:sources [:a :b :c]}}}
          producer (common-producer/->CommonProducer :in-memory producers routing nil)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (p/produce! producer {:id 1} {:topic :orders})))
      (is (= [:a :b] (mapv :id @calls))))))
