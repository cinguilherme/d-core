(ns d-core.core.authz.protocol)

(defprotocol Authorizer
  (authorize [this principal request-context opts]
    "Return {:allowed? boolean :reason keyword? :details map?}."))
