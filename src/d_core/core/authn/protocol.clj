(ns d-core.core.authn.protocol)

(defprotocol Authenticator
  (authenticate [this request opts]
    "Extract and verify credentials from request.
     Returns {:principal map :token string} or throws.")
  (verify-token [this token opts]
    "Verify a token and return a normalized principal map.")
  (challenge [this request opts]
    "Return a 401 response map for unauthenticated requests."))
