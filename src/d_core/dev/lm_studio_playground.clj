(ns d-core.dev.lm-studio-playground
  (:require [d-core.core.ai.lm-studio.openai :as lm-openai]
            [d-core.core.ai.protocol :as ai]
            [d-core.core.http.client :as http]))

(defn- env
  [key default]
  (or (System/getenv key) default))

(defn lm-studio-base-url
  []
  (env "DCORE_LM_STUDIO_URL" "http://localhost:1234/v1"))

(defn lm-studio-model
  []
  (env "DCORE_LM_STUDIO_MODEL" "qwen2.5-7b-instruct"))

(defn- default-http-client
  []
  (http/make-client {:id :lm-studio
                     :base-url (lm-studio-base-url)
                     :http-opts {:socket-timeout 60000
                                 :conn-timeout 60000}}))

(defn make-provider
  ([] (make-provider {}))
  ([opts]
   (lm-openai/->LmStudioOpenAIProvider
    (or (:http-client opts) (default-http-client))
    (or (:default-model opts) (lm-studio-model))
    (or (:capabilities opts) {})
    (or (:user-agent opts) "d-core dev/lm-studio-playground"))))

(defn smoke-text!
  "Simple text generation smoke call."
  []
  (let [provider (make-provider)]
    (ai/generate provider
                 {:messages [{:role :user
                              :content [{:type :text
                                         :text "Reply with exactly: d-core-ai-ok"}]}]
                  :output {:type :text}
                  :limits {:timeout-ms 60000}}
                 {})))

(defn smoke-structured!
  "Structured output smoke call. Expects a JSON object in assistant output."
  []
  (let [provider (make-provider)]
    (ai/generate provider
                 {:messages [{:role :system
                              :content [{:type :text
                                         :text "Return only JSON objects. No markdown."}]}
                             {:role :user
                              :content [{:type :text
                                         :text "Return {\"status\":\"ok\",\"source\":\"lm-studio\"}"}]}]
                  :output {:type :json-schema
                           :name "status_response"
                           :strict? true
                           :schema {:type "object"
                                    :properties {:status {:type "string"}
                                                 :source {:type "string"}}
                                    :required ["status" "source"]
                                    :additionalProperties false}}
                  :limits {:timeout-ms 60000}}
                 {})))

(defn smoke-vision-url!
  "Vision smoke call using an image URL."
  [image-url]
  (let [provider (make-provider)]
    (ai/generate provider
                 {:messages [{:role :user
                              :content [{:type :text
                                         :text "Describe this image in one short sentence."}
                                        {:type :image-url
                                         :url image-url}]}]
                  :output {:type :text}
                  :limits {:timeout-ms 60000}}
                 {})))

(defn smoke-vision-base64!
  "Vision smoke call using base64 image content.

  `image-base64` can be raw base64 or a full `data:` URL."
  [image-base64]
  (let [provider (make-provider)]
    (ai/generate provider
                 {:messages [{:role :user
                              :content [{:type :text
                                         :text "Describe this image in one short sentence."}
                                        {:type :image-base64
                                         :data image-base64
                                         :media-type "image/png"}]}]
                  :output {:type :text}
                  :limits {:timeout-ms 60000}}
                 {})))

(defn show-capabilities
  []
  (ai/capabilities (make-provider) {}))
