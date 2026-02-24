(ns d-core.core.state-store.protocol)

(defprotocol StateStoreProtocol
  (put-field! [this key field value opts] "Set a single field in a state hash.")
  (put-fields! [this key field->value opts] "Set multiple fields in a state hash.")
  (get-field [this key field opts] "Get one field from a state hash.")
  (get-all [this key opts] "Get all fields from a state hash as a map.")
  (delete-fields! [this key fields opts] "Delete one or more fields from a state hash.")
  (set-max-field! [this key field value opts] "Set field only when value is greater than current numeric value. Value must be numeric (number or numeric string); invalid input should fail fast.")
  (expire! [this key ttl-ms opts] "Set key expiration in milliseconds.")
  (zadd! [this key score member opts] "Add/update sorted-set member score.")
  (zcount [this key min-score max-score opts] "Count sorted-set members in score range."))
