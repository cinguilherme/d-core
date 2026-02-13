(ns d-core.core.storage.local-disk-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [d-core.core.storage.local-disk :as ld]
            [d-core.core.storage.protocol :as storage]
            [d-core.helpers.logger :as h-logger]))

;; ---------------------------------------------------------------------------
;; Fixture: temp directory per test
;; ---------------------------------------------------------------------------

(def ^:dynamic *root-path* nil)
(def ^:dynamic *sut* nil)

(defn- temp-dir-fixture [f]
  (let [tmp (java.io.File/createTempFile "ld-test" "")
        _   (.delete tmp)
        _   (.mkdirs tmp)
        {:keys [logger]} (h-logger/make-test-logger)
        sut (ld/->LocalDiskStorage (.getPath tmp) logger)]
    (binding [*root-path* (.getPath tmp)
              *sut*       sut]
      (try
        (f)
        (finally
          ;; clean up temp dir recursively
          (doseq [f (reverse (file-seq tmp))]
            (.delete ^java.io.File f)))))))

(use-fixtures :each temp-dir-fixture)

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- put! [key value]
  (storage/storage-put *sut* key value {}))

(defn- list-items [opts]
  (storage/storage-list *sut* opts))

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest empty-directory-returns-empty-list
  (testing "listing an empty storage returns ok with no items"
    (let [result (list-items {})]
      (is (:ok result))
      (is (empty? (:items result)))
      (is (not (:truncated? result))))))

(deftest basic-listing-returns-sorted-items
  (testing "items are returned sorted by key"
    (put! "charlie.txt" "c")
    (put! "alpha.txt" "a")
    (put! "bravo.txt" "b")
    (let [result (list-items {})]
      (is (:ok result))
      (is (= ["alpha.txt" "bravo.txt" "charlie.txt"]
             (mapv :key (:items result))))
      (is (not (:truncated? result)))
      (is (nil? (:next-token result))))))

(deftest listing-with-prefix-filter
  (testing "only items matching the prefix are returned"
    (put! "logs/2025-01.log" "a")
    (put! "logs/2025-02.log" "b")
    (put! "data/file.csv" "c")
    (let [result (list-items {:prefix "logs/"})]
      (is (:ok result))
      (is (= ["logs/2025-01.log" "logs/2025-02.log"]
             (mapv :key (:items result))))
      (is (not (:truncated? result))))))

(deftest pagination-with-cursor-tokens
  (testing "paginating through items using cursor-based tokens"
    ;; Create 5 files, paginate with limit=2
    (doseq [n (range 5)]
      (put! (format "file-%02d.txt" n) (str n)))

    (testing "first page returns first 2 items and a next-token"
      (let [page1 (list-items {:limit 2})]
        (is (:ok page1))
        (is (= ["file-00.txt" "file-01.txt"]
               (mapv :key (:items page1))))
        (is (:truncated? page1))
        (is (= "file-01.txt" (:next-token page1)))

        (testing "second page uses token to continue"
          (let [page2 (list-items {:limit 2 :token (:next-token page1)})]
            (is (:ok page2))
            (is (= ["file-02.txt" "file-03.txt"]
                   (mapv :key (:items page2))))
            (is (:truncated? page2))
            (is (= "file-03.txt" (:next-token page2)))

            (testing "third page returns remaining item, not truncated"
              (let [page3 (list-items {:limit 2 :token (:next-token page2)})]
                (is (:ok page3))
                (is (= ["file-04.txt"]
                       (mapv :key (:items page3))))
                (is (not (:truncated? page3)))
                (is (nil? (:next-token page3)))))))))))

(deftest pagination-with-prefix-and-cursor
  (testing "prefix and cursor combine correctly"
    (put! "a/01.txt" "1")
    (put! "a/02.txt" "2")
    (put! "a/03.txt" "3")
    (put! "b/01.txt" "x")
    (let [page1 (list-items {:prefix "a/" :limit 2})]
      (is (= ["a/01.txt" "a/02.txt"] (mapv :key (:items page1))))
      (is (:truncated? page1))
      (let [page2 (list-items {:prefix "a/" :limit 2 :token (:next-token page1)})]
        (is (= ["a/03.txt"] (mapv :key (:items page2))))
        (is (not (:truncated? page2)))))))

(deftest items-include-size-and-last-modified
  (testing "each item contains :key, :size, and :last-modified"
    (put! "test.txt" "hello")
    (let [result (list-items {})
          item   (first (:items result))]
      (is (= "test.txt" (:key item)))
      (is (= 5 (:size item)))
      (is (instance? java.util.Date (:last-modified item))))))

(deftest nonexistent-root-returns-empty-list
  (testing "storage-list on a missing root directory returns empty"
    (let [{:keys [logger]} (h-logger/make-test-logger)
          sut (ld/->LocalDiskStorage "/tmp/nonexistent-dir-xyz-12345" logger)
          result (storage/storage-list sut {})]
      (is (:ok result))
      (is (empty? (:items result)))
      (is (not (:truncated? result))))))

(deftest storage-head-existing-file-returns-metadata
  (testing "storage-head returns object metadata without reading body"
    (put! "images/cat.txt" "hello")
    (let [result (storage/storage-head *sut* "images/cat.txt" {})]
      (is (:ok result))
      (is (= "images/cat.txt" (:key result)))
      (is (= 5 (:size result)))
      ;; platform dependent; some systems may return nil
      (is (or (nil? (:content-type result))
              (string? (:content-type result))))
      (is (nil? (:etag result)))
      (is (instance? java.util.Date (:last-modified result))))))

(deftest storage-head-missing-file-returns-not-found
  (testing "storage-head returns :not-found for unknown keys"
    (let [result (storage/storage-head *sut* "missing/nope.jpg" {})]
      (is (not (:ok result)))
      (is (= :not-found (:error-type result))))))
