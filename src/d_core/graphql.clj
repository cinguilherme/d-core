(ns d-core.graphql
  (:require
   [aleph.http :as http]
   [cheshire.core :as json]
   [clojure.core.async :as a]
   [clojure.core.async.impl.protocols :as async-proto]
   [clojure.string :as str]
   [com.walmartlabs.lacinia :as lacinia]
   [com.walmartlabs.lacinia.schema :as schema]
   [duct.logger :as logger]
   [integrant.core :as ig]
   [manifold.deferred :as d]
   [manifold.stream :as s]))

(def ^:private default-graphql-path "/graphql")
(def ^:private default-graphiql-path "/graphiql")
(def ^:private default-ws-protocol "graphql-transport-ws")

(defn- compile-schema
  [schema]
  (if (map? schema)
    (schema/compile schema)
    schema))

(defn- body->string
  [body]
  (cond
    (nil? body) ""
    (string? body) body
    (bytes? body) (String. ^bytes body "UTF-8")
    :else (slurp body)))

(defn- parse-json
  [s]
  (try
    {:value (json/parse-string s true)}
    (catch Exception e
      {:error (ex-message e)})))

(defn- normalize-variables
  [variables]
  (cond
    (nil? variables) {}
    (string? variables)
    (let [s (str/trim variables)]
      (if (str/blank? s)
        {}
        (json/parse-string s true)))
    :else variables))

(defn- json-response
  [status body]
  {:status status
   :headers {"content-type" "application/json; charset=utf-8"}
   :body (json/generate-string body)})

(defn- error-response
  [status message]
  (json-response status {:errors [{:message message}]}))

(defn- graphiql-html
  [graphql-path]
  (str "<!DOCTYPE html>"
       "<html>"
       "<head>"
       "<meta charset=\"utf-8\"/>"
       "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
       "<title>GraphiQL</title>"
       "<link rel=\"stylesheet\" href=\"https://unpkg.com/graphiql/graphiql.min.css\"/>"
       "<style>body,html,#graphiql{height:100%;margin:0;width:100%;}</style>"
       "</head>"
       "<body>"
       "<div id=\"graphiql\">Loading...</div>"
       "<script crossorigin src=\"https://unpkg.com/react/umd/react.production.min.js\"></script>"
       "<script crossorigin src=\"https://unpkg.com/react-dom/umd/react-dom.production.min.js\"></script>"
       "<script crossorigin src=\"https://unpkg.com/graphiql/graphiql.min.js\"></script>"
       "<script>"
       "const graphQLFetcher = (params) => fetch('" graphql-path "', {"
       "method: 'post', headers: {'Content-Type': 'application/json'},"
       "body: JSON.stringify(params), credentials: 'same-origin'}).then(r => r.json());"
       "ReactDOM.render(React.createElement(GraphiQL, {fetcher: graphQLFetcher}),"
       "document.getElementById('graphiql'));"
       "</script>"
       "</body>"
       "</html>"))

(defn- extract-params
  [params k]
  (or (get params k)
      (get params (keyword k))))

(defn- graphql-request
  [req]
  (try
    (case (:request-method req)
      :get
      (let [params (merge (:query-params req) (:params req))
            query (extract-params params "query")
            variables (extract-params params "variables")
            operation-name (extract-params params "operationName")]
        {:query query
         :variables (normalize-variables variables)
         :operation-name operation-name})

      :post
      (let [raw (body->string (:body req))]
        (if (str/blank? raw)
          {:error "Request body is empty."}
          (let [{:keys [error value]} (parse-json raw)]
            (if error
              {:error (str "Invalid JSON body: " error)}
              (let [query (or (:query value) (get value "query"))
                    variables (or (:variables value) (get value "variables"))
                    operation-name (or (:operationName value)
                                       (:operation-name value)
                                       (get value "operationName"))]
                {:query query
                 :variables (normalize-variables variables)
                 :operation-name operation-name}))))))

    {:error "Unsupported HTTP method."}
    (catch Exception e
      {:error (str "Invalid variables JSON: " (ex-message e))})))

(defn- async-chan?
  [x]
  (satisfies? async-proto/ReadPort x))

(defn- send-ws!
  [conn msg]
  (s/put! conn (json/generate-string msg)))

(defn- send-ws-error!
  [conn id message]
  (send-ws! conn {:type "error"
                  :id id
                  :payload [{:message message}]}))

(defn- send-ws-next!
  [conn id payload]
  (send-ws! conn {:type "next" :id id :payload payload}))

(defn- send-ws-complete!
  [conn id]
  (send-ws! conn {:type "complete" :id id}))

(defn- stop-subscription!
  [subscriptions id]
  (when-let [{:keys [stop]} (get @subscriptions id)]
    (stop)
    (swap! subscriptions dissoc id)))

(defn- stop-all-subscriptions!
  [subscriptions]
  (doseq [id (keys @subscriptions)]
    (stop-subscription! subscriptions id)))

(defn- attach-manifold-stream!
  [conn subscriptions id stream]
  (let [stop #(s/close! stream)]
    (swap! subscriptions assoc id {:stop stop})
    (s/consume (fn [payload] (send-ws-next! conn id payload)) stream)
    (s/on-closed stream
                 (fn []
                   (swap! subscriptions dissoc id)
                   (send-ws-complete! conn id)))))

(defn- attach-async-chan!
  [conn subscriptions id ch]
  (let [stop #(a/close! ch)]
    (swap! subscriptions assoc id {:stop stop})
    (a/go-loop []
      (if-some [payload (a/<! ch)]
        (do
          (send-ws-next! conn id payload)
          (recur))
        (do
          (swap! subscriptions dissoc id)
          (send-ws-complete! conn id))))))

(defn- decode-ws-message
  [raw]
  (let [s (cond
            (string? raw) raw
            (bytes? raw) (String. ^bytes raw "UTF-8")
            :else (str raw))]
    (json/parse-string s true)))

(defn- default-execute
  [{:keys [schema query variables operation-name context]}]
  (let [opts (cond-> {}
               operation-name (assoc :operation-name operation-name))]
    (lacinia/execute schema query variables context opts)))

(defn- build-context
  [context context-fn req]
  (if context-fn
    (context-fn req)
    context))

(defn- handle-ws-subscribe
  [conn subscriptions state {:keys [schema execute context context-fn]} id payload]
  (cond
    (nil? id)
    (send-ws-error! conn id "Missing subscription id.")

    (get @subscriptions id)
    (send-ws-error! conn id "Subscription id already in use.")

    (str/blank? (:query payload))
    (send-ws-error! conn id "Missing GraphQL query.")

    :else
    (try
      (let [context (build-context context context-fn
                                   {:type :ws
                                    :connection conn
                                    :payload payload
                                    :connection-init (:connection-init @state)})
            result (execute {:schema schema
                             :query (:query payload)
                             :variables (normalize-variables (:variables payload))
                             :operation-name (or (:operationName payload)
                                                 (:operation-name payload))
                             :context context})]
        (cond
          (s/stream? result) (attach-manifold-stream! conn subscriptions id result)
          (async-chan? result) (attach-async-chan! conn subscriptions id result)
          :else (do
                  (send-ws-next! conn id result)
                  (send-ws-complete! conn id))))
      (catch Exception e
        (send-ws-error! conn id (ex-message e))
        (send-ws-complete! conn id)))))

(defn- handle-graphql-transport-ws
  [conn {:keys [schema execute context context-fn logger]}]
  (let [subscriptions (atom {})
        state (atom {:ack? false})]
    (s/on-closed conn #(stop-all-subscriptions! subscriptions))
    (s/consume
      (fn [raw]
        (let [{:keys [type id payload]} (decode-ws-message raw)]
          (case type
            "connection_init"
            (do
              (swap! state assoc :ack? true :connection-init payload)
              (send-ws! conn {:type "connection_ack"}))

            "ping"
            (send-ws! conn {:type "pong" :payload payload})

            "pong"
            nil

            "subscribe"
            (handle-ws-subscribe conn subscriptions state
                                 {:schema schema
                                  :execute execute
                                  :context context
                                  :context-fn context-fn
                                  :logger logger}
                                 id
                                 payload)

            "complete"
            (stop-subscription! subscriptions id)

            (do
              (when logger
                (logger/log logger :warn ::unknown-ws-message {:type type}))
              (send-ws-error! conn id (str "Unsupported message type: " type))))))
      conn)
    conn))

(defn- websocket-protocol-allowed?
  [req ws-protocol]
  (let [header (get-in req [:headers "sec-websocket-protocol"])]
    (if (and ws-protocol header)
      (some #(= ws-protocol %) (map str/trim (str/split header #",")))
      true)))

(defn- maybe-handle-ws
  [req {:keys [ws-protocol] :as opts}]
  (when (websocket-protocol-allowed? req ws-protocol)
    (when-let [ws (http/websocket-connection req {:protocols [ws-protocol]})]
      (d/let-flow [conn ws]
        (handle-graphql-transport-ws conn opts)
        conn))))

(defn- handle-graphql-http
  [{:keys [schema execute context context-fn]} req]
  (let [{:keys [query variables operation-name error]} (graphql-request req)]
    (cond
      error (error-response 400 error)
      (str/blank? query) (error-response 400 "Missing GraphQL query.")
      :else
      (try
        (let [context (build-context context context-fn req)
              result (execute {:schema schema
                               :query query
                               :variables variables
                               :operation-name operation-name
                               :context context})]
          (if (or (s/stream? result) (async-chan? result))
            (error-response 400 "Subscriptions require WebSocket transport.")
            (json-response 200 result)))
        (catch Exception e
          (error-response 500 (ex-message e)))))))

(defmethod ig/init-key :d-core.graphql/handler
  [_ {:keys [schema logger graphql-path graphiql? graphiql-path subscriptions? ws-protocol
             context context-fn execute]
      :or {graphql-path default-graphql-path
           graphiql-path default-graphiql-path
           graphiql? true
           subscriptions? true
           ws-protocol default-ws-protocol}}]
  (let [compiled-schema (compile-schema schema)
        execute (or execute default-execute)]
    (when logger
      (logger/log logger :info ::graphql-handler-initialized
                  {:path graphql-path
                   :graphiql? (boolean graphiql?)
                   :subscriptions? (boolean subscriptions?)}))
    (fn [req]
      (cond
        (and subscriptions? (= (:uri req) graphql-path))
        (if-let [ws-response (maybe-handle-ws req {:schema compiled-schema
                                                   :execute execute
                                                   :context context
                                                   :context-fn context-fn
                                                   :logger logger
                                                   :ws-protocol ws-protocol})]
          ws-response
          (handle-graphql-http {:schema compiled-schema
                                :execute execute
                                :context context
                                :context-fn context-fn} req))

        (= (:uri req) graphql-path)
        (handle-graphql-http {:schema compiled-schema
                              :execute execute
                              :context context
                              :context-fn context-fn} req)

        (and graphiql? (= (:uri req) graphiql-path))
        {:status 200
         :headers {"content-type" "text/html; charset=utf-8"}
         :body (graphiql-html graphql-path)}

        :else
        {:status 404
         :headers {"content-type" "text/plain; charset=utf-8"}
         :body "Not Found"}))))

(defmethod ig/init-key :d-core.graphql/server
  [_ {:keys [port handler logger]}]
  (when logger
    (logger/log logger :info ::graphql-server-starting {:port port}))
  {:server (http/start-server handler {:port port})
   :port port
   :logger logger})

(defmethod ig/halt-key! :d-core.graphql/server
  [_ server]
  (let [{:keys [server logger port]} (if (map? server) server {:server server})]
    (when logger
      (logger/log logger :info ::graphql-server-stopping
                  (cond-> {} port (assoc :port port))))
    (.close server)))
