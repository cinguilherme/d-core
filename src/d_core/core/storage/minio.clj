(ns d-core.core.storage.minio
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [d-core.core.storage.protocol :as p]))

(import (com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials)
        (com.amazonaws ClientConfiguration)
        (com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration)
        (com.amazonaws.services.s3 AmazonS3 AmazonS3ClientBuilder)
        (com.amazonaws.services.s3.model AmazonS3Exception
                                         ListObjectsV2Request
                                         ObjectMetadata
                                         PutObjectRequest))

(def ^:private byte-array-class (class (byte-array 0)))

(defn- bytes?
  [value]
  (instance? byte-array-class value))

(defn- ensure-bucket!
  [^AmazonS3 client bucket logger]
  (try
    (when-not (.doesBucketExistV2 client bucket)
      (logger/log logger :info ::creating-bucket {:bucket bucket})
      (.createBucket client bucket))
    (catch Exception e
      (logger/log logger :error ::ensure-bucket-failed {:bucket bucket :error (.getMessage e)})
      (throw e))))

(defn- build-s3-client
  [{:keys [endpoint access-key secret-key connection-timeout-ms socket-timeout-ms]}]
  (let [conn-ms (long (or connection-timeout-ms 10000))
        sock-ms (long (or socket-timeout-ms 10000))
        client-config (doto (ClientConfiguration.)
                        (.setConnectionTimeout conn-ms)
                        (.setSocketTimeout sock-ms))
        creds (AWSStaticCredentialsProvider.
               (BasicAWSCredentials. access-key secret-key))
        ;; MinIO generally expects path-style access for localhost endpoints.
        builder (doto (AmazonS3ClientBuilder/standard)
                  (.withCredentials creds)
                  (.withClientConfiguration client-config)
                  (.withPathStyleAccessEnabled true)
                  (.withEndpointConfiguration (AwsClientBuilder$EndpointConfiguration.
                                              endpoint
                                              "us-east-1")))]
    (.build builder)))

(defrecord MinioStorage [^AmazonS3 client bucket logger]
  p/StorageProtocol
  (storage-get [this key opts]
    (let [result (p/storage-get-bytes this key opts)]
      (if (:ok result)
        {:ok true :key key :value (String. ^bytes (:bytes result) "UTF-8")}
        (dissoc result :bytes))))
  (storage-put [this key value opts]
    (let [bytes-value? (bytes? value)
          content-type (or (:content-type opts)
                           (if bytes-value?
                             "application/octet-stream"
                             "application/json"))
          ^bytes bytes (if bytes-value?
                         value
                         (.getBytes (str value) "UTF-8"))]
      (p/storage-put-bytes this key bytes (assoc opts :content-type content-type))))
  (storage-delete [_ key _opts]
    (try
      (.deleteObject client bucket key)
      {:ok true :key key :bucket bucket}
      (catch Exception e
        (logger/log logger :error ::storage-delete-failed {:key key :error (.getMessage e)})
        {:ok false :key key :bucket bucket :error (.getMessage e) :error-type :error})))
  (storage-get-bytes [_ key _opts]
    (try
      (with-open [obj (.getObject client bucket key)
                  stream (.getObjectContent obj)
                  out (java.io.ByteArrayOutputStream.)]
        (let [buf (byte-array 4096)]
          (loop [n (.read stream buf)]
            (when (pos? n)
              (.write out buf 0 n)
              (recur (.read stream buf)))))
        {:ok true
         :key key
         :bucket bucket
         :bytes (.toByteArray out)})
      (catch AmazonS3Exception e
        (let [error-code (.getErrorCode e)
              status (.getStatusCode e)
              not-found? (or (= "NoSuchKey" error-code)
                             (= "NotFound" error-code)
                             (= 404 status))
              log-level (if not-found? :info :error)]
          (logger/log logger log-level ::storage-get-failed {:key key
                                                            :error (.getMessage e)
                                                            :error-code error-code
                                                            :status status})
          {:ok false
           :key key
           :bucket bucket
           :error (.getMessage e)
           :error-code error-code
           :status status
           :error-type (if not-found? :not-found :s3-error)}))
      (catch Exception e
        (logger/log logger :error ::storage-get-failed {:key key :error (.getMessage e)})
        {:ok false :key key :bucket bucket :error (.getMessage e)})))
  (storage-put-bytes [_ key bytes opts]
    (try
      (let [content-type (or (:content-type opts) "application/octet-stream")
            meta (doto (ObjectMetadata.)
                   (.setContentLength (alength ^bytes bytes))
                   (.setContentType content-type))
            req (PutObjectRequest.
                 bucket
                 key
                 (java.io.ByteArrayInputStream. bytes)
                 meta)]
        (.putObject client req)
        {:ok true :key key :bucket bucket :content-type content-type})
      (catch Exception e
        (logger/log logger :error ::storage-put-failed {:key key :error (.getMessage e)})
        {:ok false :key key :bucket bucket :error (.getMessage e)})))
  (storage-head [_ key _opts]
    (try
      (let [meta (.getObjectMetadata client bucket key)]
        {:ok true
         :key key
         :bucket bucket
         :size (.getContentLength meta)
         :content-type (.getContentType meta)
         :etag (.getETag meta)
         :last-modified (.getLastModified meta)})
      (catch AmazonS3Exception e
        (let [error-code (.getErrorCode e)
              status (.getStatusCode e)
              not-found? (or (= "NoSuchKey" error-code)
                             (= "NotFound" error-code)
                             (= 404 status))
              log-level (if not-found? :info :error)]
          (logger/log logger log-level ::storage-head-failed {:key key
                                                              :error (.getMessage e)
                                                              :error-code error-code
                                                              :status status})
          {:ok false
           :key key
           :bucket bucket
           :error (.getMessage e)
           :error-code error-code
           :status status
           :error-type (if not-found? :not-found :s3-error)}))
      (catch Exception e
        (logger/log logger :error ::storage-head-failed {:key key :error (.getMessage e)})
        {:ok false :key key :bucket bucket :error (.getMessage e) :error-type :error})))
  (storage-list [_ {:keys [prefix limit token]}]
    (try
      (let [req (doto (ListObjectsV2Request.)
                  (.setBucketName bucket)
                  (.setPrefix (or prefix ""))
                  (.setMaxKeys (int (or limit 50))))
            _ (when (seq token)
                (.setContinuationToken req token))
            resp (.listObjectsV2 client req)
            summaries (.getObjectSummaries resp)
            items (mapv (fn [summary]
                          {:key (.getKey summary)
                           :size (.getSize summary)
                           :last-modified (.getLastModified summary)})
                        summaries)]
        {:ok true
         :items items
         :prefix (.getPrefix resp)
         :truncated? (.isTruncated resp)
         :next-token (.getNextContinuationToken resp)})
      (catch Exception e
        (logger/log logger :error ::storage-list-failed {:error (.getMessage e)})
        {:ok false :error (.getMessage e)}))))

(defmethod ig/init-key :d-core.core.storage/minio
  [_ {:keys [endpoint access-key secret-key bucket logger connection-timeout-ms socket-timeout-ms]}]
  (logger/log logger :info ::initializing-minio-storage {:endpoint endpoint :bucket bucket})
  (let [client (build-s3-client {:endpoint endpoint
                                 :access-key access-key
                                 :secret-key secret-key
                                 :connection-timeout-ms connection-timeout-ms
                                 :socket-timeout-ms socket-timeout-ms})]
    (ensure-bucket! client bucket logger)
    (->MinioStorage client bucket logger)))
