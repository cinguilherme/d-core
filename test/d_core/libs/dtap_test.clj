(ns d-core.libs.dtap-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.libs.dtap :refer [dtap dtap*]]))

(deftest dtap-prints-and-returns
  (let [value {:a 1}
        out (with-out-str (is (= value (dtap* {:header? false :color? false} value))))]
    (is (= "{:a 1}\n" out))))

(deftest dtap-header-from-form
  (let [out (with-out-str (is (= 3 (dtap {:color? false} (+ 1 2)))))]
    (is (= "dtap: (+ 1 2)\n3\n" out))))

(deftest dtap-header-label
  (testing "string labels are not pr-str'd"
    (let [out (with-out-str (dtap* {:label "x" :color? false} 1 nil))]
      (is (= "dtap: x\n1\n" out))))
  (testing "non-string labels are pr-str'd"
    (let [out (with-out-str (dtap* {:label :x :color? false} 1 nil))]
      (is (= "dtap: :x\n1\n" out)))))

(deftest dtap-header-disabled
  (let [out (with-out-str (dtap* {:show-form? false :color? false} 1 "(+ 1 2)"))]
    (is (= "1\n" out))))

(deftest dtap-max-depth
  (let [value (array-map :a (array-map :b 1))
        out (with-out-str (dtap* {:header? false :color? false :max-depth 1} value))]
    (is (= "{:a #<max-depth>}\n" out))))

(deftest dtap-max-items
  (testing "vectors are truncated"
    (let [out (with-out-str (dtap* {:header? false :color? false :max-items 2} [1 2 3]))]
      (is (= "[1 2 ...]\n" out))))
  (testing "maps are truncated"
    (let [value (array-map :a 1 :b 2 :c 3)
          out (with-out-str (dtap* {:header? false :color? false :max-items 2} value))]
      (is (= "{:a 1 :b 2 ...}\n" out)))))

(deftest dtap-out-writer
  (let [w (java.io.StringWriter.)]
    (is (= [1 2] (dtap* {:header? false :color? false :out w} [1 2])))
    (is (= "[1 2]\n" (str w)))))

(deftest dtap-tap-forwarding
  (let [p (promise)
        tap-fn (fn [v] (deliver p v))]
    (add-tap tap-fn)
    (try
      (dtap* {:header? false :color? false :tap? true} {:a 1})
      (is (= {:a 1} (deref p 1000 ::timeout)))
      (finally
        (remove-tap tap-fn)))))
