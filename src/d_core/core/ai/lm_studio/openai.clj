(ns d-core.core.ai.lm-studio.openai
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [d-core.core.ai.protocol :as p]
            [d-core.core.ai.schema :as schema]
            [d-core.core.http.client :as http]
            [integrant.core :as ig]))

(def ^:private default-capabilities
  {:provider :lm-studio-openai
   :input-modalities #{:text :image}
   :output-types #{:text :json-schema}
   :streaming? false
   :structured-output? true
   :vision? true})

(defn- non-empty-map
  [value]
  (when (and (map? value) (seq value))
    value))

(defn- response->json
  [response]
  (let [body (:body response)]
    (cond
      (map? body) body
      (string? body) (if (str/blank? body)
                       nil
                       (json/parse-string body true))
      :else nil)))

(defn- error-message
  [body]
  (or (when (string? (:error body))
        (:error body))
      (get-in body [:error :message])
      (:message body)
      ""))

(defn- mapped-error-type
  [status body]
  (let [message (str/lower-case (or (error-message body) ""))]
    (cond
      (= status 429)
      :d-core.ai/rate-limited

      (or (= status 404)
          (re-find #"model.+not\s+found" message))
      :d-core.ai/model-not-found

      (re-find #"(?i)(context|token limit|max tokens|maximum context|context window|too long)"
               message)
      :d-core.ai/context-too-large

      (re-find #"(?i)(unsupported|not support|response[_ -]?format|json schema|vision|image input)"
               message)
      :d-core.ai/unsupported-capability

      :else
      :d-core.ai/provider-error)))

(defn- throw-provider-error!
  [message response body]
  (throw (ex-info message
                  {:type (mapped-error-type (:status response) body)
                   :provider :lm-studio-openai
                   :status (:status response)
                   :error-message (error-message body)
                   :response response
                   :body body})))

(defn- normalize-keyword
  [value]
  (cond
    (keyword? value) value
    (string? value) (-> value
                        str/trim
                        str/lower-case
                        (str/replace #"[_\s]+" "-")
                        keyword)
    :else nil))

(defn- parse-nat-int
  [value]
  (cond
    (and (integer? value) (>= value 0))
    value

    (and (number? value) (>= value 0))
    (long value)

    (string? value)
    (try
      (let [parsed (Long/parseLong value)]
        (when (>= parsed 0)
          parsed))
      (catch NumberFormatException _ nil))

    :else nil))

(defn- content-part->openai
  [{:keys [type text url data media-type]}]
  (case type
    :text
    {:type "text"
     :text text}

    :image-url
    {:type "image_url"
     :image_url {:url url}}

    :image-base64
    {:type "image_url"
     :image_url {:url (if (str/starts-with? data "data:")
                        data
                        (str "data:" (or media-type "image/png") ";base64," data))}}

    (throw (ex-info "Unsupported content part type"
                    {:type :d-core.ai/unsupported-capability
                     :provider :lm-studio-openai
                     :content-part-type type}))))

(defn- message->openai
  [{:keys [role content tool-call-id]
    :as message}]
  (let [message-name (:name message)]
    (cond-> {:role (name role)
             :content (mapv content-part->openai content)}
      message-name (assoc :name message-name)
      tool-call-id (assoc :tool_call_id tool-call-id))))

(defn- output->response-format
  [{:keys [type schema strict? name]}]
  (when (= :json-schema type)
    {:type "json_schema"
     :json_schema (cond-> {:name (or name "dcore_output")
                           :schema schema}
                    (some? strict?) (assoc :strict strict?))}))

(defn- build-payload
  [{:keys [default-model]} {:keys [model messages output sampling limits]}]
  (let [model (or model default-model)]
    (when-not model
      (throw (ex-info "AI generation requires :model or provider :default-model"
                      {:type :d-core.ai/provider-error
                       :provider :lm-studio-openai
                       :error-message "Missing model"})))
    (cond-> {:model model
             :messages (mapv message->openai messages)
             :stream false}
      (:temperature sampling) (assoc :temperature (:temperature sampling))
      (:top-p sampling) (assoc :top_p (:top-p sampling))
      (:frequency-penalty sampling) (assoc :frequency_penalty (:frequency-penalty sampling))
      (:presence-penalty sampling) (assoc :presence_penalty (:presence-penalty sampling))
      (:seed sampling) (assoc :seed (:seed sampling))
      (:max-output-tokens limits) (assoc :max_tokens (:max-output-tokens limits))
      (= :json-schema (:type output)) (assoc :response_format (output->response-format output)))))

(defn- request!
  [{:keys [http-client user-agent]} payload opts timeout-ms]
  (let [request {:method :post
                 :path "/chat/completions"
                 :as :text
                 :headers {"Accept" "application/json"
                           "Content-Type" "application/json"
                           "User-Agent" user-agent}
                 :body (json/generate-string payload)}
        request (cond-> request
                  timeout-ms (assoc :socket-timeout timeout-ms
                                    :conn-timeout timeout-ms))]
    (http/request! http-client
                   (cond-> request
                     (:request opts) (merge (:request opts))))))

(defn- content->text
  [content]
  (cond
    (string? content)
    content

    (vector? content)
    (->> content
         (map (fn [part]
                (cond
                  (string? part) part
                  (map? part) (or (:text part)
                                  (get part "text"))
                  :else nil)))
         (remove nil?)
         (apply str))

    :else
    nil))

(defn- parse-structured-output
  [text]
  (when-not (string? text)
    (throw (ex-info "Structured output requires assistant text content"
                    {:type :d-core.ai/provider-error
                     :provider :lm-studio-openai
                     :error-message "Missing text content for structured output"})))
  (try
    (let [value (json/parse-string text true)]
      (if (map? value)
        value
        (throw (ex-info "Structured output must be a JSON object"
                        {:type :d-core.ai/provider-error
                         :provider :lm-studio-openai
                         :error-message "Structured output is not a JSON object"
                         :structured value}))))
    (catch clojure.lang.ExceptionInfo ex
      (throw ex))
    (catch Exception ex
      (throw (ex-info "Failed to parse structured output JSON"
                      {:type :d-core.ai/provider-error
                       :provider :lm-studio-openai
                       :error-message "Invalid JSON in structured output"
                       :text text}
                      ex)))))

(defn- choice->item
  [choice output]
  (let [message (:message choice)
        text (or (content->text (:content message))
                 (when (string? (:text choice))
                   (:text choice)))
        role (normalize-keyword (:role message))
        finish-reason (normalize-keyword (:finish_reason choice))]
    (cond-> {:text text
             :index (or (:index choice) 0)
             :finish-reason finish-reason
             :role (or role :assistant)}
      (= :json-schema (:type output))
      (assoc :structured (parse-structured-output text)))))

(defn- usage->normalized
  [usage]
  (non-empty-map
   {:input-tokens (parse-nat-int (:prompt_tokens usage))
    :output-tokens (parse-nat-int (:completion_tokens usage))
    :total-tokens (parse-nat-int (:total_tokens usage))}))

(defn- response->envelope
  [request body]
  (let [choices (:choices body)]
    (when-not (seq choices)
      (throw (ex-info "LM Studio response is missing :choices"
                      {:type :d-core.ai/provider-error
                       :provider :lm-studio-openai
                       :error-message "Missing choices"
                       :body body})))
    (schema/validate-generate-envelope!
     (cond-> {:items (mapv #(choice->item % (:output request)) choices)
              :provider :lm-studio-openai}
       (:usage body) (assoc :usage (usage->normalized (:usage body)))
       (or (:model body) (:id body))
       (assoc :metadata (non-empty-map
                         {:model (:model body)
                          :request-id (:id body)}))))))

(defrecord LmStudioOpenAIProvider [http-client default-model capabilities user-agent]
  p/GenerationProtocol
  (generate [this request opts]
    (let [request (schema/validate-generate-request! request)
          payload (build-payload this request)
          timeout-ms (get-in request [:limits :timeout-ms])
          response (request! this payload opts timeout-ms)
          body (response->json response)]
      (if (<= 200 (:status response) 299)
        (response->envelope request (or body {}))
        (throw-provider-error! "LM Studio OpenAI generation request failed" response body))))

  p/ModelCapabilitiesProtocol
  (capabilities [_ _opts]
    (schema/validate-capabilities-envelope!
     (merge default-capabilities capabilities))))

(defmethod ig/init-key :d-core.core.ai.lm-studio.openai/provider
  [_ {:keys [http-client default-model capabilities user-agent] :as opts}]
  (when-not http-client
    (throw (ex-info "LM Studio OpenAI provider requires :http-client" {:opts opts})))
  (->LmStudioOpenAIProvider http-client
                            default-model
                            (or capabilities {})
                            (or user-agent "d-core ai/lm-studio-openai")))
