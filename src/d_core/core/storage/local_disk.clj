(ns d-core.core.storage.local-disk
  (:require [integrant.core :as ig]
            [clojure.java.io :as io]
            [duct.logger :as logger]
            [d-core.core.storage.protocol :as p])
  (:import [java.nio.file Files OpenOption]))

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
        {:ok false :key key :path (.getPath file) :error :not-found})))
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
          limit (long (or limit 50))
          offset (or (some-> token str parse-long) 0)
          root (io/file root-path)
          files (if (.exists root)
                  (let [root-path-obj (.toPath (.getCanonicalFile root))]
                    (->> (file-seq root)
                         (filter #(.isFile ^java.io.File %))
                         (map (fn [^java.io.File f]
                                (let [f-path-obj (.toPath (.getCanonicalFile f))
                                      rel (.relativize root-path-obj f-path-obj)]
                                  {:key (.toString rel)
                                   :size (.length f)
                                   :last-modified (java.util.Date. (.lastModified f))})))
                         (filter (fn [{:keys [key]}]
                                   (or (empty? prefix)
                                       (.startsWith ^String key prefix))))
                         (sort-by :key)
                         vec))
                  [])
          selected (->> files (drop offset) (take limit) vec)
          next-offset (+ offset (count selected))
          truncated? (< next-offset (count files))]
      {:ok true
       :items selected
       :prefix prefix
       :truncated? truncated?
       :next-token (when truncated? (str next-offset))})))

(defmethod ig/init-key :d-core.core.storage/local-disk
  [_ {:keys [root-path logger] :or {root-path "storage"}}]
  (logger/log logger :info ::initializing-local-disk-storage {:root-path root-path})
  (let [dir (io/file root-path)]
    (when-not (.exists dir)
      (.mkdirs dir)))
  (->LocalDiskStorage root-path logger))
