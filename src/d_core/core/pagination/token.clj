(ns d-core.core.pagination.token
  (:require [cheshire.core :as json]
            [clojure.string :as str])
  (:import (java.util Base64)))

(defn encode-token
  [token]
  (let [payload (json/generate-string token)
        bytes (.getBytes payload "UTF-8")]
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bytes)))

(defn decode-token
  [token]
  (when (and (string? token) (not (str/blank? token)))
    (try
      (let [bytes (.decode (Base64/getUrlDecoder) token)
            payload (String. ^bytes bytes "UTF-8")]
        (json/parse-string payload true))
      (catch Exception _
        nil))))

(defn decode-token-map
  [token]
  (let [decoded (decode-token token)]
    (when (map? decoded)
      decoded)))
