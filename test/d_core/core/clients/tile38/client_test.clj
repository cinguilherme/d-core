(ns d-core.core.clients.tile38.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.tile38.client :as t38]))

(deftest point-normalization
  (testing "point normalization accepts map or vector forms"
    (is (= [1 2] (#'t38/point->latlon {:lat 1 :lon 2})))
    (is (= [1 2] (#'t38/point->latlon {:latitude 1 :longitude 2})))
    (is (= [1 2] (#'t38/point->latlon [1 2])))))

(deftest bounds-normalization
  (testing "bounds normalization accepts map or vector forms"
    (is (= [1 2 3 4] (#'t38/bounds->coords {:minlat 1 :minlon 2 :maxlat 3 :maxlon 4})))
    (is (= [1 2 3 4] (#'t38/bounds->coords [1 2 3 4])))))

(deftest field-args-shape
  (testing "fields map expands into FIELD args"
    (let [args (#'t38/fields->args {:name "truck" :speed 12})]
      (is (= 6 (count args)))
      (is (= #{"FIELD"} (set (take-nth 3 args))))
      (is (= #{"name" "speed"} (set (take-nth 3 (rest args))))))))

(deftest json-parsing
  (testing "JSON responses are parsed when they look like JSON"
    (is (= {:ok true} (#'t38/maybe-parse-json "{\"ok\":true}")))
    (is (= "OK" (#'t38/maybe-parse-json "OK")))))
