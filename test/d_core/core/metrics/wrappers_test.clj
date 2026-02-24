(ns d-core.core.metrics.wrappers-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.metrics.wrappers :as wrappers]))

(deftest record-redis-updates-counter-and-histogram
  (testing "record-redis! labels and writes through metrics api"
    (let [calls (atom [])
          metrics-api :metrics
          counter-sample (fn [labels]
                           (swap! calls conj {:labels (vec labels)})
                           :counter-labeled)
          hist-sample (fn [labels]
                        (swap! calls conj {:labels (vec labels)})
                        :hist-labeled)]
      (with-redefs [d-core.core.metrics.protocol/inc! (fn [_ metric]
                                                        (swap! calls conj {:inc metric})
                                                        :ok)
                    d-core.core.metrics.protocol/observe! (fn [_ metric value]
                                                            (swap! calls conj {:observe metric :value value})
                                                            :ok)]
        (wrappers/record-redis! metrics-api {:requests-total counter-sample
                                             :request-duration hist-sample}
                                :hgetall 0.01 :ok)
        (is (= [{:labels ["hgetall" "ok"]}
                {:inc :counter-labeled}
                {:labels ["hgetall"]}
                {:observe :hist-labeled :value 0.01}]
               @calls))))))

(deftest with-redis-success-and-error
  (testing "with-redis records status on success and error"
    (let [calls (atom [])]
      (with-redefs [d-core.core.metrics.wrappers/record-redis! (fn [_ _ op _ status]
                                                                 (swap! calls conj {:op op :status status}))]
        (is (= :ok (wrappers/with-redis :metrics :instruments :set (fn [] :ok))))
        (is (thrown? Exception (wrappers/with-redis :metrics :instruments :set (fn [] (throw (Exception. "boom"))))))
        (is (= [{:op :set :status :ok}
                {:op :set :status :error}]
               @calls))))))
