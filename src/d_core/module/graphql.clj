(ns d-core.module.graphql
  (:require
   [integrant.core :as ig]))

(defmethod ig/expand-key :d-core.module/graphql
  [_ {:keys [schema port logger graphql-path graphiql? graphiql-path graphiql-assets subscriptions?
             ws-protocol context context-fn execute]
      :or {graphql-path "/graphql"
           graphiql-path "/graphiql"
           graphiql? true
           subscriptions? true
           ws-protocol "graphql-transport-ws"}}]
  `{:d-core.graphql/handler
    {:schema ~schema
     :graphql-path ~graphql-path
     :graphiql? ~graphiql?
     :graphiql-path ~graphiql-path
     :graphiql-assets ~graphiql-assets
     :subscriptions? ~subscriptions?
     :ws-protocol ~ws-protocol
     :context ~context
     :context-fn ~context-fn
     :execute ~execute
     :logger ~logger}

    :d-core.graphql/server
    {:port ~port
     :handler ~(ig/ref :d-core.graphql/handler)
     :logger ~logger}})
