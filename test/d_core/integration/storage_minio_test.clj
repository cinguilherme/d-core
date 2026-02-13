(ns d-core.integration.storage-minio-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.helpers.logger :as h-logger]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.storage.minio]
            [integrant.core :as ig])
  (:import (java.util UUID)))

(defn- integration-enabled?
  []
  (or (some? (System/getenv "INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION"))
      (some? (System/getenv "DCORE_INTEGRATION_STORAGE"))))

(defn- minio-endpoint
  []
  (or (System/getenv "DCORE_MINIO_ENDPOINT")
      "http://localhost:9000"))

(defn- minio-access-key
  []
  (or (System/getenv "DCORE_MINIO_ACCESS_KEY")
      "minio"))

(defn- minio-secret-key
  []
  (or (System/getenv "DCORE_MINIO_SECRET_KEY")
      "minio123"))

(defn- minio-bucket
  []
  (or (System/getenv "DCORE_MINIO_BUCKET")
      "dcore-storage-test"))

(defn- init-system
  [logger]
  (ig/init {:d-core.core.storage/minio {:endpoint (minio-endpoint)
                                        :access-key (minio-access-key)
                                        :secret-key (minio-secret-key)
                                        :bucket (minio-bucket)
                                        :logger logger}}))

(deftest integration-minio-storage-head
  (testing "storage-head checks object metadata without downloading bytes"
    (if-not (integration-enabled?)
      (is true "Skipping MinIO storage-head integration test; set INTEGRATION=1")
      (let [logger (:logger (h-logger/make-test-logger))
            system (init-system logger)
            minio (:d-core.core.storage/minio system)
            key (str "dcore.int.head." (UUID/randomUUID))
            payload (.getBytes "hello-minio-head" "UTF-8")]
        (try
          (is (:ok (storage/storage-put-bytes minio key payload {:content-type "text/plain"})))

          (let [head-result (storage/storage-head minio key nil)]
            (is (:ok head-result))
            (is (= key (:key head-result)))
            (is (= (alength ^bytes payload) (:size head-result))))

          (let [missing-result (storage/storage-head minio (str key ".missing") nil)]
            (is (not (:ok missing-result)))
            (is (= :not-found (:error-type missing-result))))
          (finally
            (storage/storage-delete minio key nil)
            (ig/halt! system)))))))
