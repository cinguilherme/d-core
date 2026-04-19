(ns d-core.core.clients.kubernetes.client-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.clients.kubernetes.client :as client]
            [d-core.core.http.client :as http])
  (:import (java.io File)
           (java.security KeyStore)))

(def ^:private test-ca-pem
  "-----BEGIN CERTIFICATE-----
MIIDEzCCAfugAwIBAgIUSvn1xeenk0vZdwmJvVRqNfVlGe0wDQYJKoZIhvcNAQEL
BQAwGTEXMBUGA1UEAwwOZGNvcmUtazhzLXRlc3QwHhcNMjYwNDE5MDEwMzM4WhcN
MjYwNDIwMDEwMzM4WjAZMRcwFQYDVQQDDA5kY29yZS1rOHMtdGVzdDCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMHaShgSN255djImszkNQ6HQj403H64P
3vR7t+WCwiGXzlng5ZWw4hiJ16RBJCxps52B/BLGuxOCb9446v5edDPks9mgNi2X
VBuN6qbE4mFudLy6D8P6ilGwogdwESq2mMQm40Ho+YCKkULMBh1syHyZPIIS/CI9
5RLH/whEqeQ/ZZutY2zOtfLgeicOURl6M65lKyXYu1WLf/j0rfJK+lkZDw4rE32i
i/bYuyEvDQDAYYWmHzzjjiK9FOIiH62+AQA0l8LLKr6EitNB6Xn9tK2N6U1la6BO
DUONRh53v8jdkj1ueVcuhjuJGnCXNYNQ6JTeAS7uoYtqrFuP7ucGh5UCAwEAAaNT
MFEwHQYDVR0OBBYEFFQ1fDCtYPHCiXbSP9vuOjFE8C4xMB8GA1UdIwQYMBaAFFQ1
fDCtYPHCiXbSP9vuOjFE8C4xMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQEL
BQADggEBAH6GGhIN//ToyvtGNtIJy6+rjA7msfO/PwHts1C4QL1Ah5Q1boOfrrRH
H685FQNxHn8+pr7f83yAfJn+k+wxo/umMJnQyqWII3ieKkv97hpq+P7Cv9C1j1DS
WzwxygpAZzkWUc0rgAdTgdGaoQ1dlPOwrsVHLT/7SParG8pG94EIU+0IJ26k1lJI
nYmY5svL6IsFTnQiu9F+ZG8L68GpRZuyV5Ykn4lEUU+MniwN/yLXUFaAv3jpOvA5
7SCu9mCAMI9ktMzATboafuuSZLTHcooYs1qfnAVSX3A+xKrr3RoxUKmZMvs1HEuX
JPc+oWY1Ll++ct3qxZFatHNciPhdZnA=
-----END CERTIFICATE-----")

(defn- temp-file
  [prefix suffix content]
  (let [f (File/createTempFile prefix suffix)]
    (.deleteOnExit f)
    (spit f content)
    (.getAbsolutePath f)))

(deftest make-client-discovers-in-cluster-context
  (let [token-file (temp-file "k8s-token" ".txt" "token-1\n")
        ca-file (temp-file "k8s-ca" ".pem" test-ca-pem)
        namespace-file (temp-file "k8s-ns" ".txt" "workers\n")]
    (with-redefs [client/env (fn [k]
                               (case k
                                 "KUBERNETES_SERVICE_HOST" "10.0.0.15"
                                 "KUBERNETES_SERVICE_PORT_HTTPS" "6443"
                                 nil))]
      (let [k8s-client (client/make-client {:token-file token-file
                                            :ca-cert-file ca-file
                                            :namespace-file namespace-file
                                            :request-timeout-ms 2500})
            ^KeyStore trust-store (get-in k8s-client [:http-client :http-opts :trust-store])]
        (is (= "https://10.0.0.15:6443" (:api-server-url k8s-client)))
        (is (= "workers" (:namespace k8s-client)))
        (is (= 2500 (:request-timeout-ms k8s-client)))
        (is (instance? KeyStore trust-store))
        (is (= 1 (.size trust-store)))))))

(deftest make-client-allows-namespace-override
  (let [token-file (temp-file "k8s-token" ".txt" "token-1\n")
        ca-file (temp-file "k8s-ca" ".pem" test-ca-pem)]
    (let [k8s-client (client/make-client {:api-server-url "https://kubernetes.default.svc:443"
                                          :token-file token-file
                                          :ca-cert-file ca-file
                                          :namespace "override-ns"})]
      (is (= "override-ns" (:namespace k8s-client))))))

(deftest request-rereads-token-and-parses-json-body
  (let [token-file (temp-file "k8s-token" ".txt" "token-a\n")
        ca-file (temp-file "k8s-ca" ".pem" test-ca-pem)
        requests (atom [])
        k8s-client (client/make-client {:api-server-url "https://kubernetes.default.svc:443"
                                        :token-file token-file
                                        :ca-cert-file ca-file
                                        :namespace "workers"})]
    (with-redefs [http/request! (fn [_ request]
                                  (swap! requests conj request)
                                  {:status 200
                                   :body (json/generate-string {:kind "Status"
                                                                :status "Success"})})]
      (is (= {:kind "Status" :status "Success"}
             (:body (client/request! k8s-client {:method :get
                                                 :path "/apis"}))))
      (spit token-file "token-b\n")
      (client/request! k8s-client {:method :get :path "/apis"})
      (is (= "Bearer token-a" (get-in @requests [0 :headers "Authorization"])))
      (is (= "Bearer token-b" (get-in @requests [1 :headers "Authorization"])))
      (is (= "application/json" (get-in @requests [0 :headers "Accept"]))))))
