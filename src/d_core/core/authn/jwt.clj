(ns d-core.core.authn.jwt
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.set :as set]
   [clojure.string :as str]
   [duct.logger :as logger]
   [integrant.core :as ig]
   [d-core.core.authn.protocol :as p])
  (:import
   (java.math BigInteger)
   (java.security KeyFactory Signature)
   (java.security.spec RSAPublicKeySpec)
   (java.util Base64)))

(defn- now-ms []
  (System/currentTimeMillis))

(defn- base64url-decode
  ^bytes [^String s]
  (.decode (Base64/getUrlDecoder) s))

(defn- decode-json-segment
  [segment]
  (json/parse-string (String. (base64url-decode segment) "UTF-8") true))

(defn- parse-jwt
  [token]
  (let [parts (str/split token #"\.")]
    (when-not (= 3 (count parts))
      (throw (ex-info "Invalid JWT format" {:type ::invalid-token})))
    (let [[header payload signature] parts]
      {:header (decode-json-segment header)
       :claims (decode-json-segment payload)
       :signing-input (str header "." payload)
       :signature-bytes (base64url-decode signature)})))

(defn- b64url->bigint
  [s]
  (BigInteger. 1 (base64url-decode s)))

(defn- jwk->rsa-public-key
  [{:keys [n e kty]}]
  (when-not (= "RSA" kty)
    (throw (ex-info "Unsupported JWK type" {:type ::unsupported-jwk
                                            :kty kty})))
  (let [spec (RSAPublicKeySpec. (b64url->bigint n) (b64url->bigint e))]
    (.generatePublic (KeyFactory/getInstance "RSA") spec)))

(defn- verify-rs256
  [public-key signing-input signature]
  (let [sig (Signature/getInstance "SHA256withRSA")]
    (.initVerify sig public-key)
    (.update sig (.getBytes signing-input "UTF-8"))
    (.verify sig signature)))

(defn- fetch-jwks
  [jwks-uri http-opts]
  (let [resp (http/get jwks-uri (merge {:as :text
                                        :throw-exceptions false}
                                       http-opts))]
    (when-not (= 200 (:status resp))
      (throw (ex-info "Failed to fetch JWKS"
                      {:type ::jwks-fetch-failed
                       :status (:status resp)})))
    (json/parse-string (:body resp) true)))

(defn- cache-fresh?
  [snapshot now ttl-ms]
  (let [{:keys [jwks fetched-at]} snapshot]
    (and jwks fetched-at (< (- now fetched-at) ttl-ms))))

(defn- await-inflight
  [p]
  (let [result @p]
    (if (instance? Throwable result)
      (throw result)
      result)))

(defn- refresh-jwks!
  [state current snapshot jwks-uri http-opts now]
  (let [p (promise)
        next-state (assoc snapshot :inflight p)]
    (if (compare-and-set! state current next-state)
      (try
        (let [fresh (fetch-jwks jwks-uri http-opts)]
          (deliver p fresh)
          (reset! state {:jwks fresh :fetched-at now})
          fresh)
        (catch Exception ex
          (deliver p ex)
          (swap! state dissoc :inflight)
          (throw ex)))
      ::retry)))

(defn- get-jwks
  [{:keys [jwks jwks-uri jwks-cache-ttl-ms http-opts state]}]
  (if jwks
    jwks
    (do
      (when-not jwks-uri
        (throw (ex-info "jwks-uri is required when jwks is not provided"
                        {:type ::missing-jwks-uri})))
      (let [ttl-ms (long (or jwks-cache-ttl-ms 300000))]
        (loop []
          (let [current @state
                snapshot (or current {})
                now (now-ms)
                cached-jwks (:jwks snapshot)]
            (cond
              (cache-fresh? snapshot now ttl-ms)
              cached-jwks

              (:inflight snapshot)
              (await-inflight (:inflight snapshot))

              :else
              (let [result (refresh-jwks! state current snapshot jwks-uri http-opts now)]
                (if (= ::retry result)
                  (recur)
                  result)))))))))

(defn- match-issuer?
  [claims issuer]
  (if issuer
    (= issuer (:iss claims))
    true))

(defn- normalize-aud
  [aud]
  (cond
    (nil? aud) #{}
    (string? aud) #{aud}
    (coll? aud) (->> aud (map str) set)
    :else #{(str aud)}))

(defn- match-audience?
  [claims aud]
  (if aud
    (let [required (normalize-aud aud)
          token-aud (normalize-aud (:aud claims))]
      (boolean (seq (set/intersection required token-aud))))
    true))

(defn- within-time?
  [claims clock-skew-ms]
  (let [skew-sec (long (/ (or clock-skew-ms 60000) 1000))
        now-sec (long (/ (now-ms) 1000))
        to-long (fn [v]
                  (cond
                    (nil? v) nil
                    (number? v) (long v)
                    (string? v) (try
                                  (Long/parseLong v)
                                  (catch NumberFormatException _
                                    (throw (ex-info "Invalid numeric claim"
                                                    {:type ::invalid-claim
                                                     :value v}))))
                    :else nil))
        exp (to-long (:exp claims))
        nbf (to-long (:nbf claims))
        exp-ok (if exp (> (+ exp skew-sec) now-sec) true)
        nbf-ok (if nbf (< (- nbf skew-sec) now-sec) true)]
    (and exp-ok nbf-ok)))

(defn- normalize-scopes
  [scopes]
  (cond
    (nil? scopes) #{}
    (set? scopes) scopes
    (string? scopes) (->> (str/split scopes #"\s+") (remove str/blank?) set)
    (coll? scopes) (->> scopes (map str) set)
    :else #{(str scopes)}))

(defn- build-principal
  [{:keys [claims]} {:keys [tenant-claim scope-claim]}]
  (let [tenant-claim (or tenant-claim "tenant_id")
        scope-claim (or scope-claim "scope")
        tenant-id (get claims (keyword tenant-claim) (get claims tenant-claim))
        raw-scopes (get claims (keyword scope-claim) (get claims scope-claim))]
    {:subject (:sub claims)
     :tenant-id tenant-id
     :scopes (normalize-scopes raw-scopes)
     :client-id (:azp claims)
     :aud (:aud claims)
     :issuer (:iss claims)
     :expires-at (when-let [exp (:exp claims)] (* 1000 exp))
     :actor (:act claims)
     :claims claims}))

(defn- resolve-key
  [jwks header]
  (let [kid (:kid header)
        keys (:keys jwks)]
    (when-not kid
      (throw (ex-info "JWT kid missing" {:type ::missing-kid})))
    (or (some #(when (= kid (:kid %)) %) keys)
        (throw (ex-info "JWK not found" {:type ::jwk-not-found
                                         :kid kid})))))

(defn- verify-jwt
  [{:keys [issuer aud jwks jwks-uri clock-skew-ms] :as opts} token]
  (let [{:keys [header claims signing-input signature-bytes]} (parse-jwt token)
        alg (:alg header)]
    (when-not (= "RS256" alg)
      (throw (ex-info "Unsupported JWT alg" {:type ::unsupported-alg
                                             :alg alg})))
    (when-not (match-issuer? claims issuer)
      (throw (ex-info "Issuer mismatch" {:type ::issuer-mismatch})))
    (when-not (match-audience? claims aud)
      (throw (ex-info "Audience mismatch" {:type ::audience-mismatch})))
    (when-not (within-time? claims clock-skew-ms)
      (throw (ex-info "JWT expired or not active" {:type ::token-expired})))
    (let [jwks (get-jwks (assoc opts :jwks jwks :jwks-uri jwks-uri))
          jwk (resolve-key jwks header)
          public-key (jwk->rsa-public-key jwk)]
      (when-not (verify-rs256 public-key signing-input signature-bytes)
        (throw (ex-info "JWT signature invalid" {:type ::invalid-signature})))
      {:header header :claims claims})))

(defn- bearer-token
  [request]
  (when-let [auth (get-in request [:headers "authorization"])]
    (let [auth (str/trim auth)]
      (when (str/starts-with? (str/lower-case auth) "bearer ")
        (str/trim (subs auth 7))))))

(defrecord JwtAuthenticator
  [issuer aud jwks jwks-uri jwks-cache-ttl-ms clock-skew-ms
   tenant-claim scope-claim http-opts logger state]
  p/Authenticator
  (authenticate [this request opts]
    (let [token (or (bearer-token request) (:token opts))]
      (when-not token
        (throw (ex-info "Missing bearer token" {:type ::missing-token})))
      (let [{:keys [claims]} (verify-jwt this token)
            principal (build-principal {:claims claims} this)]
        {:principal principal
         :token token})))
  (verify-token [this token _opts]
    (let [{:keys [claims]} (verify-jwt this token)]
      (build-principal {:claims claims} this)))
  (challenge [_ request _opts]
    (let [host (get-in request [:headers "host"])
          realm (or issuer host "d-core")]
      {:status 401
       :headers {"WWW-Authenticate" (str "Bearer realm=\"" realm "\"")}
       :body "Unauthorized"})))

(defmethod ig/init-key :d-core.core.authn.jwt/authenticator
  [_ {:keys [issuer aud jwks jwks-uri jwks-cache-ttl-ms clock-skew-ms
             tenant-claim scope-claim http-opts logger]}]
  (when logger
    (logger/log logger :info ::jwt-authenticator-initialized
                {:issuer issuer
                 :aud aud
                 :jwks-uri jwks-uri}))
  (->JwtAuthenticator issuer aud jwks jwks-uri jwks-cache-ttl-ms clock-skew-ms
                      tenant-claim scope-claim http-opts logger (atom nil)))
