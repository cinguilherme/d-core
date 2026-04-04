(ns d-core.core.ai.schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.ai.schema :as schema]))

(deftest validate-generate-request-normalization
  (testing "request normalizes roles, content types, and defaults output to text"
    (let [result (schema/validate-generate-request!
                  {:model "qwen2.5"
                   :messages [{:role "USER"
                               :content [{:type "TEXT"
                                          :text "Hello"}]}
                              {:role :assistant
                               :content [{:type "image_url"
                                          :url "https://example.com/cat.png"}]}]})]
      (is (= :user (get-in result [:messages 0 :role])))
      (is (= :text (get-in result [:messages 0 :content 0 :type])))
      (is (= :image-url (get-in result [:messages 1 :content 0 :type])))
      (is (= {:type :text} (:output result)))))

  (testing "json-schema output defaults strict? to true"
    (let [result (schema/validate-generate-request!
                  {:messages [{:role :user
                               :content [{:type :text
                                          :text "Return JSON"}]}]
                   :output {:type :json_schema
                            :schema {:type "object"
                                     :properties {:status {:type "string"}}}}})]
      (is (= :json-schema (get-in result [:output :type])))
      (is (= true (get-in result [:output :strict?]))))))

(deftest validate-generate-request-vision-inputs
  (testing "image-url and image-base64 content parts are accepted"
    (let [result (schema/validate-generate-request!
                  {:messages [{:role :user
                               :content [{:type :text :text "Describe"}
                                         {:type :image-url :url "https://example.com/a.png"}
                                         {:type :image-base64 :data "QUJDRA==" :media-type "IMAGE/PNG"}]}]})]
      (is (= :image-url (get-in result [:messages 0 :content 1 :type])))
      (is (= :image-base64 (get-in result [:messages 0 :content 2 :type])))
      (is (= "image/png" (get-in result [:messages 0 :content 2 :media-type]))))))

(deftest validate-generate-request-invalid-shapes
  (testing "unknown content part types fail strict validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-generate-request!
                  {:messages [{:role :user
                               :content [{:type :audio :url "https://example.com/a.mp3"}]}]}))))

  (testing "blank text content fails validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-generate-request!
                  {:messages [{:role :user
                               :content [{:type :text :text "   "}]}]})))))

(deftest validate-generate-envelope-rules
  (testing "text and structured generation items are accepted"
    (let [result (schema/validate-generate-envelope!
                  {:items [{:text "hello" :finish-reason "stop" :role "assistant"}
                           {:text "{\"status\":\"ok\"}"
                            :structured {:status "ok"}
                            :finish-reason :stop}]
                   :provider :lm-studio-openai
                   :usage {:input-tokens 5
                           :output-tokens 2
                           :total-tokens 7}})]
      (is (= :stop (get-in result [:items 0 :finish-reason])))
      (is (= :assistant (get-in result [:items 0 :role])))
      (is (= {:status "ok"} (get-in result [:items 1 :structured])))))

  (testing "item without text or structured payload fails validation"
    (is (thrown? clojure.lang.ExceptionInfo
                 (schema/validate-generate-envelope!
                  {:items [{:finish-reason :stop}]
                   :provider :lm-studio-openai})))))

(deftest validate-capabilities-envelope-normalization
  (testing "capabilities normalize modality/output sets and default booleans"
    (let [result (schema/validate-capabilities-envelope!
                  {:provider :lm-studio-openai
                   :input-modalities ["TEXT" "image"]
                   :output-types ["text" "json_schema"]})]
      (is (= #{:text :image} (:input-modalities result)))
      (is (= #{:text :json-schema} (:output-types result)))
      (is (= false (:streaming? result)))
      (is (= false (:structured-output? result)))
      (is (= false (:vision? result))))))
