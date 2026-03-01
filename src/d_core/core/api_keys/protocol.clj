(ns d-core.core.api-keys.protocol)

(defprotocol ApiKeyStore
  (ensure-schema! [this opts]
    "Ensures backend storage required for API keys exists.")
  (create-key! [this data opts]
    "Creates a new API key.

     Expected data:
     - :name (optional) string
     - :tenant-id (required) string/keyword
     - :scopes (optional) coll of strings/keywords
     - :expires-at (optional) java.time.Instant/java.util.Date
     - :limits (optional) map
     - :metadata (optional) map

     Returns:
     {:api-key map
      :token string} where :token is shown once.")
  (get-key [this api-key-id opts]
    "Returns one API key metadata map or nil.")
  (list-keys [this filters opts]
    "Lists API keys. Filters may include :tenant-id, :status, :limit, :offset.")
  (revoke-key! [this api-key-id opts]
    "Revokes an API key. Returns {:revoked? boolean}.")
  (rotate-key! [this api-key-id opts]
    "Rotates secret material for an existing key.
     Returns {:api-key map :token string} where :token is shown once.")
  (authenticate-key [this token opts]
    "Validates a presented key token.
     Returns a normalized map with identity + policy fields, or nil."))
