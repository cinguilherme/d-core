(ns d-core.core.storage.protocol)

(defprotocol StorageProtocol
  (storage-get [this key opts] "Get a value from the storage")
  (storage-put [this key value opts] "Set a value in the storage")
  (storage-delete [this key opts] "Delete a value from the storage")
  (storage-get-bytes [this key opts] "Get raw bytes from storage")
  (storage-put-bytes [this key bytes opts] "Set raw bytes in storage")
  (storage-head [this key opts] "Fetch object metadata/existence without body")
  (storage-list [this opts] "List stored objects"))
