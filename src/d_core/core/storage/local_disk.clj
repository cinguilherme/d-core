(ns d-core.core.storage.local-disk
  (:require [integrant.core :as ig]
            [clojure.java.io :as io]
            [duct.logger :as logger]
            [d-core.core.storage.protocol :as p])
  (:import [java.nio.file Files FileVisitOption LinkOption OpenOption]))

(defrecord LocalDiskStorage [root-path logger]
  p/StorageProtocol
  (storage-get [_ key _opts]
    (let [file (io/file root-path key)]
      (if (.exists file)
        {:ok true :key key :value (slurp file)}
        (do
          (logger/log logger :warn ::file-not-found {:path (.getPath file)})
          {:ok false :key key :error-type :not-found}))))
  (storage-put [_ key value _opts]
    (let [file (io/file root-path key)]
      (io/make-parents file)
      (spit file value)
      {:ok true :key key :path (.getPath file)}))
  (storage-delete [_ key _opts]
    (let [file (io/file root-path key)]
      (if (.exists file)
        (do
          (io/delete-file file)
          {:ok true :key key :path (.getPath file)})
        {:ok false :key key :path (.getPath file) :error :not-found :error-type :not-found})))
  (storage-get-bytes [_ key _opts]
    (let [file (io/file root-path key)]
      (if (.exists file)
        {:ok true
         :key key
         :path (.getPath file)
         :bytes (Files/readAllBytes (.toPath file))}
        (do
          (logger/log logger :warn ::file-not-found {:path (.getPath file)})
          {:ok false
           :key key
           :path (.getPath file)
           :error :not-found
           :error-type :not-found}))))
  (storage-put-bytes [_ key bytes _opts]
    (let [file (io/file root-path key)]
      (io/make-parents file)
      (Files/write (.toPath file) ^bytes bytes (into-array OpenOption []))
      {:ok true :key key :path (.getPath file)}))
  (storage-list [_ {:keys [prefix limit token]}]
    (let [prefix (or prefix "")
          limit  (long (or limit 50))
          root   (io/file root-path)
          root-p (.toPath root)]
      (if-not (.exists root)
        {:ok true :items [] :prefix prefix :truncated? false}
        (with-open [stream (Files/walk root-p (into-array FileVisitOption []))]
          (let [no-follow (into-array LinkOption [])
                rel-keys (->> (.iterator stream)
                              iterator-seq
                              (filter #(Files/isRegularFile % no-follow))
                              (map #(str (.relativize root-p %)))
                              (filter #(or (empty? prefix)
                                           (.startsWith ^String % prefix)))
                              (filter #(or (nil? token)
                                           (pos? (compare % token))))
                              sort
                              (take (inc limit))
                              vec)
                truncated? (> (count rel-keys) limit)
                selected   (if truncated? (pop rel-keys) rel-keys)
                items      (mapv (fn [rel-key]
                                   (let [p (.resolve root-p rel-key)
                                         f (.toFile p)]
                                     {:key           rel-key
                                      :size          (.length f)
                                      :last-modified (java.util.Date.
                                                       (.lastModified f))}))
                                 selected)]
            {:ok true
             :items items
             :prefix prefix
             :truncated? truncated?
             :next-token (when truncated? (:key (last items)))}))))))

(defmethod ig/init-key :d-core.core.storage/local-disk
  [_ {:keys [root-path logger] :or {root-path "storage"}}]
  (logger/log logger :info ::initializing-local-disk-storage {:root-path root-path})
  (let [dir (io/file root-path)]
    (when-not (.exists dir)
      (.mkdirs dir)))
  (->LocalDiskStorage root-path logger))
