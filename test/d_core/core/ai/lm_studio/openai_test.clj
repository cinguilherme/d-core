(ns d-core.core.ai.lm-studio.openai-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.ai.lm-studio.openai :as sut]
            [d-core.core.ai.protocol :as p]
            [d-core.core.http.client :as http]))

(defn- make-http-client
  []
  (http/make-client {:id :lm-studio
                     :base-url "http://lm-studio.local/v1"}))

(defn- make-provider
  []
  (sut/->LmStudioOpenAIProvider
   (make-http-client)
   "qwen2.5-7b-instruct"
   {:provider :lm-studio-openai
    :input-modalities #{:text :image}
    :output-types #{:text :json-schema}
    :streaming? false
    :structured-output? true
    :vision? true}
   "d-core-tests"))

(defn- parse-request-body
  [request]
  (json/parse-string (:body request) true))

(deftest generate-text-success
  (testing "text generation maps request and normalizes response envelope"
    (let [request* (atom nil)
          provider (make-provider)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:id "chatcmpl-1"
                                             :model "qwen2.5-7b-instruct"
                                             :choices [{:index 0
                                                        :finish_reason "stop"
                                                        :message {:role "assistant"
                                                                  :content "d-core-ai-ok"}}]
                                             :usage {:prompt_tokens 8
                                                     :completion_tokens 3
                                                     :total_tokens 11}})})]
        (let [result (p/generate provider
                                 {:messages [{:role :user
                                              :content [{:type :text
                                                         :text "Reply with exactly d-core-ai-ok"}]}]
                                  :output {:type :text}}
                                 {})
              payload (parse-request-body @request*)]
          (is (= "/chat/completions" (:path @request*)))
          (is (= "qwen2.5-7b-instruct" (:model payload)))
          (is (= "user" (get-in payload [:messages 0 :role])))
          (is (= "text" (get-in payload [:messages 0 :content 0 :type])))
          (is (= "Reply with exactly d-core-ai-ok"
                 (get-in payload [:messages 0 :content 0 :text])))
          (is (= :lm-studio-openai (:provider result)))
          (is (= "d-core-ai-ok" (get-in result [:items 0 :text])))
          (is (= :stop (get-in result [:items 0 :finish-reason])))
          (is (= {:input-tokens 8 :output-tokens 3 :total-tokens 11}
                 (:usage result))))))))

(deftest generate-structured-output-success
  (testing "json-schema output sends response_format and parses structured result"
    (let [request* (atom nil)
          provider (make-provider)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:choices [{:index 0
                                                        :finish_reason "stop"
                                                        :message {:role "assistant"
                                                                  :content "{\"status\":\"ok\"}"}}]})})]
        (let [result (p/generate provider
                                 {:messages [{:role :user
                                              :content [{:type :text
                                                         :text "Return {\"status\":\"ok\"}"}]}]
                                  :output {:type :json-schema
                                           :schema {:type "object"
                                                    :properties {:status {:type "string"}}
                                                    :required ["status"]
                                                    :additionalProperties false}}}
                                 {})
              payload (parse-request-body @request*)]
          (is (= "json_schema" (get-in payload [:response_format :type])))
          (is (= "dcore_output"
                 (get-in payload [:response_format :json_schema :name])))
          (is (= {:status "ok"} (get-in result [:items 0 :structured])))
          (is (= "{\"status\":\"ok\"}" (get-in result [:items 0 :text]))))))))

(deftest generate-vision-input-mapping
  (testing "vision content maps image-url and image-base64 to OpenAI-style content"
    (let [request* (atom nil)
          provider (make-provider)]
      (with-redefs [http/request! (fn [_ request]
                                    (reset! request* request)
                                    {:status 200
                                     :body (json/generate-string
                                            {:choices [{:index 0
                                                        :finish_reason "stop"
                                                        :message {:role "assistant"
                                                                  :content "A cat on a couch."}}]})})]
        (p/generate provider
                    {:messages [{:role :user
                                 :content [{:type :text
                                            :text "Describe the image"}
                                           {:type :image-url
                                            :url "https://example.com/cat.png"}
                                           {:type :image-base64
                                            :data "QUJDRA=="
                                            :media-type "image/png"}]}]
                     :output {:type :text}}
                    {})
        (let [payload (parse-request-body @request*)]
          (is (= "image_url" (get-in payload [:messages 0 :content 1 :type])))
          (is (= "https://example.com/cat.png"
                 (get-in payload [:messages 0 :content 1 :image_url :url])))
          (is (= "data:image/png;base64,QUJDRA=="
                 (get-in payload [:messages 0 :content 2 :image_url :url]))))))))

(deftest generate-error-mapping
  (testing "model-not-found maps to typed error"
    (let [provider (make-provider)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 404
                                     :body (json/generate-string {:error {:message "Model not found"}})})]
        (try
          (p/generate provider {:messages [{:role :user :content [{:type :text :text "hi"}]}]} {})
          (is false "Expected typed provider exception")
          (catch clojure.lang.ExceptionInfo ex
            (is (= :d-core.ai/model-not-found (:type (ex-data ex)))))))))

  (testing "429 maps to rate-limited"
    (let [provider (make-provider)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 429
                                     :body (json/generate-string {:error {:message "Rate limit exceeded"}})})]
        (try
          (p/generate provider {:messages [{:role :user :content [{:type :text :text "hi"}]}]} {})
          (is false "Expected typed provider exception")
          (catch clojure.lang.ExceptionInfo ex
            (is (= :d-core.ai/rate-limited (:type (ex-data ex)))))))))

  (testing "context overflows map to context-too-large"
    (let [provider (make-provider)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 400
                                     :body (json/generate-string {:error {:message "maximum context length exceeded"}})})]
        (try
          (p/generate provider {:messages [{:role :user :content [{:type :text :text "hi"}]}]} {})
          (is false "Expected typed provider exception")
          (catch clojure.lang.ExceptionInfo ex
            (is (= :d-core.ai/context-too-large (:type (ex-data ex))))))))))

(deftest generate-malformed-provider-payload-fails
  (testing "missing choices fails with provider error"
    (let [provider (make-provider)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string {:id "chatcmpl-2"})})]
        (try
          (p/generate provider {:messages [{:role :user :content [{:type :text :text "hi"}]}]} {})
          (is false "Expected malformed payload exception")
          (catch clojure.lang.ExceptionInfo ex
            (is (= :d-core.ai/provider-error (:type (ex-data ex)))))))))

  (testing "invalid structured output JSON fails with provider error"
    (let [provider (make-provider)]
      (with-redefs [http/request! (fn [_ _]
                                    {:status 200
                                     :body (json/generate-string
                                            {:choices [{:index 0
                                                        :finish_reason "stop"
                                                        :message {:role "assistant"
                                                                  :content "not-json"}}]})})]
        (try
          (p/generate provider
                      {:messages [{:role :user
                                   :content [{:type :text :text "Return JSON"}]}]
                       :output {:type :json-schema
                                :schema {:type "object"
                                         :properties {:status {:type "string"}}}}}
                      {})
          (is false "Expected structured output parse exception")
          (catch clojure.lang.ExceptionInfo ex
            (is (= :d-core.ai/provider-error (:type (ex-data ex))))))))))

(deftest capabilities-defaults
  (testing "capabilities return normalized provider metadata"
    (let [provider (make-provider)
          caps (p/capabilities provider {})]
      (is (= :lm-studio-openai (:provider caps)))
      (is (= #{:text :image} (:input-modalities caps)))
      (is (= #{:text :json-schema} (:output-types caps)))
      (is (= false (:streaming? caps)))
      (is (= true (:structured-output? caps)))
      (is (= true (:vision? caps))))))
