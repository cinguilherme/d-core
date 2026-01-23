(ns d-core.core.clients.sqs.client
  (:require [clojure.string :as str])
  (:import (com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials)
           (com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration)
           (com.amazonaws.services.sqs AmazonSQS AmazonSQSClientBuilder)
           (com.amazonaws.services.sqs.model CreateQueueRequest DeleteMessageRequest DeleteQueueRequest
                                              GetQueueUrlRequest Message MessageAttributeValue
                                              PurgeQueueRequest QueueDoesNotExistException
                                              ReceiveMessageRequest SendMessageRequest)
           (java.util Map)))

(defrecord SQSClient [^AmazonSQS client config]
  Object
  (toString [_]
    (str "#SQSClient" (dissoc config :secret-key))))

(defn- local-endpoint?
  [endpoint]
  (when endpoint
    (let [endpoint (str/lower-case (str endpoint))]
      (or (str/includes? endpoint "localhost")
          (str/includes? endpoint "127.0.0.1")))))

(defn- maybe-credentials-provider
  [{:keys [access-key secret-key credentials-provider endpoint]}]
  (cond
    credentials-provider credentials-provider
    (and access-key secret-key)
    (AWSStaticCredentialsProvider. (BasicAWSCredentials. access-key secret-key))

    (local-endpoint? endpoint)
    (AWSStaticCredentialsProvider. (BasicAWSCredentials. "x" "x"))

    :else nil))

(defn make-client
  [{:keys [endpoint region access-key secret-key credentials-provider]
    :or {endpoint "http://localhost:9324"
         region "us-east-1"}}]
  (let [provider (maybe-credentials-provider {:access-key access-key
                                              :secret-key secret-key
                                              :credentials-provider credentials-provider
                                              :endpoint endpoint})
        builder (cond-> (AmazonSQSClientBuilder/standard)
                  provider (.withCredentials provider)
                  endpoint (.withEndpointConfiguration
                             (AwsClientBuilder$EndpointConfiguration. endpoint region))
                  (and (nil? endpoint) region) (.withRegion region))
        client (.build builder)]
    (->SQSClient client {:endpoint endpoint
                         :region region
                         :access-key access-key
                         :secret-key secret-key})))

(defn close!
  [^SQSClient client]
  (when-let [^AmazonSQS sqs (:client client)]
    (try
      (.shutdown sqs)
      (catch Exception _e
        nil))))

(defn- attrs->java-map
  [attrs]
  (when (seq attrs)
    (into {} (map (fn [[k v]] [(if (keyword? k) (name k) (str k)) (str v)]) attrs))))

(defn create-queue!
  [^SQSClient client queue-name {:keys [attributes]}]
  (let [req (CreateQueueRequest. (str queue-name))]
    (when (seq attributes)
      (.setAttributes req ^Map (attrs->java-map attributes)))
    (let [resp (.createQueue ^AmazonSQS (:client client) req)]
      (.getQueueUrl resp))))

(defn queue-url
  [^SQSClient client queue-name]
  (try
    (let [req (GetQueueUrlRequest. (str queue-name))
          resp (.getQueueUrl ^AmazonSQS (:client client) req)]
      (.getQueueUrl resp))
    (catch QueueDoesNotExistException _e
      nil)))

(defn delete-queue!
  [^SQSClient client queue-url]
  (let [req (DeleteQueueRequest. (str queue-url))]
    (.deleteQueue ^AmazonSQS (:client client) req)
    :ok))

(defn purge-queue!
  [^SQSClient client queue-url]
  (let [req (PurgeQueueRequest. (str queue-url))]
    (.purgeQueue ^AmazonSQS (:client client) req)
    :ok))

(defn- attribute-value
  [v]
  (let [attr (MessageAttributeValue.)]
    (cond
      (map? v)
      (do
        (when-let [dt (:data-type v)]
          (.setDataType attr (str dt)))
        (when-let [sv (:string-value v)]
          (.setStringValue attr (str sv)))
        (when-let [bv (:binary-value v)]
          (.setBinaryValue attr bv))
        (when-let [svs (:string-list-values v)]
          (.setStringListValues attr (mapv str svs)))
        (when-let [bvs (:binary-list-values v)]
          (.setBinaryListValues attr bvs))
        attr)

      :else
      (doto attr
        (.setDataType "String")
        (.setStringValue (str v))))))

(defn- message-attributes->java
  [attrs]
  (when (seq attrs)
    (into {}
          (map (fn [[k v]] [(if (keyword? k) (name k) (str k)) (attribute-value v)]))
          attrs)))

(defn send-message!
  [^SQSClient client queue-url body {:keys [delay-seconds message-attributes]}]
  (let [req (SendMessageRequest. (str queue-url) (str body))]
    (when delay-seconds
      (.setDelaySeconds req (int delay-seconds)))
    (when (seq message-attributes)
      (.setMessageAttributes req ^Map (message-attributes->java message-attributes)))
    (let [resp (.sendMessage ^AmazonSQS (:client client) req)]
      {:message-id (.getMessageId resp)
       :md5 (.getMD5OfMessageBody resp)})))

(defn- message-attribute->map
  [^MessageAttributeValue v]
  (cond-> {:data-type (.getDataType v)}
    (.getStringValue v) (assoc :string-value (.getStringValue v))
    (.getBinaryValue v) (assoc :binary-value (.getBinaryValue v))
    (seq (.getStringListValues v)) (assoc :string-list-values (vec (.getStringListValues v)))
    (seq (.getBinaryListValues v)) (assoc :binary-list-values (vec (.getBinaryListValues v)))))

(defn- message-attributes->map
  [attrs]
  (when (seq attrs)
    (into {}
          (map (fn [[k v]] [k (message-attribute->map v)]))
          attrs)))

(defn- message->map
  [^Message msg]
  {:message-id (.getMessageId msg)
   :receipt-handle (.getReceiptHandle msg)
   :body (.getBody msg)
   :attributes (when (seq (.getAttributes msg)) (into {} (.getAttributes msg)))
   :message-attributes (message-attributes->map (.getMessageAttributes msg))})

(defn receive-messages!
  [^SQSClient client queue-url {:keys [max-messages wait-seconds visibility-timeout
                                       attribute-names message-attribute-names]
                                :or {max-messages 1 wait-seconds 10}}]
  (let [req (ReceiveMessageRequest. (str queue-url))]
    (.setMaxNumberOfMessages req (int max-messages))
    (.setWaitTimeSeconds req (int wait-seconds))
    (when visibility-timeout
      (.setVisibilityTimeout req (int visibility-timeout)))
    (when (seq attribute-names)
      (.setAttributeNames req (mapv name attribute-names)))
    (when (seq message-attribute-names)
      (.setMessageAttributeNames req (mapv name message-attribute-names)))
    (let [resp (.receiveMessage ^AmazonSQS (:client client) req)
          messages (.getMessages resp)]
      (mapv message->map messages))))

(defn delete-message!
  [^SQSClient client queue-url receipt-handle]
  (let [req (DeleteMessageRequest. (str queue-url) (str receipt-handle))]
    (.deleteMessage ^AmazonSQS (:client client) req)
    :ok))
