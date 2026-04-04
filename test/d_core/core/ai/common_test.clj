(ns d-core.core.ai.common-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.ai.common :as sut]
            [d-core.core.ai.protocol :as p]))

(defn- stub-provider
  [provider-id calls*]
  (reify
    p/GenerationProtocol
    (generate [_ request opts]
      (swap! calls* conj {:provider provider-id
                          :method :generate
                          :request request
                          :opts opts})
      {:items [{:text "ok"}]
       :provider provider-id})

    p/ModelCapabilitiesProtocol
    (capabilities [_ opts]
      (swap! calls* conj {:provider provider-id
                          :method :capabilities
                          :opts opts})
      {:provider provider-id
       :input-modalities #{:text}
       :output-types #{:text}
       :streaming? false
       :structured-output? false
       :vision? false})))

(deftest common-ai-default-provider
  (testing "default provider is used when opts does not set :provider"
    (let [calls* (atom [])
          ai (sut/init-common {:default-provider :a
                               :providers {:a (stub-provider :a calls*)
                                           :b (stub-provider :b calls*)}})]
      (is (= :a (:provider (p/generate ai {:messages [] :output {:type :text}} {}))))
      (is (= :a (:provider (p/capabilities ai {}))))
      (is (= [:generate :capabilities]
             (mapv :method @calls*))))))

(deftest common-ai-provider-override
  (testing "opts :provider routes calls to explicit backend"
    (let [calls* (atom [])
          ai (sut/init-common {:default-provider :a
                               :providers {:a (stub-provider :a calls*)
                                           :b (stub-provider :b calls*)}})]
      (is (= :b (:provider (p/generate ai {:messages [] :output {:type :text}} {:provider :b}))))
      (is (= :b (:provider (p/capabilities ai {:provider :b}))))
      (is (every? #(= :b (:provider %)) @calls*)))))

(deftest common-ai-unknown-provider
  (testing "unknown provider fails with useful ex-data"
    (let [ai (sut/init-common {:default-provider :a
                               :providers {:a (stub-provider :a (atom []))}})]
      (try
        (p/generate ai {:messages [] :output {:type :text}} {:provider :missing})
        (is false "Expected unknown provider exception")
        (catch clojure.lang.ExceptionInfo ex
          (is (= :missing (:provider (ex-data ex))))
          (is (= [:a] (vec (:known (ex-data ex))))))))))
