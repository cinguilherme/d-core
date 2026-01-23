(ns d-core.core.criptography.protocol)

(defprotocol CriptographyProtocol
  (encrypt [this value] "Encrypt a value")
  (decrypt [this value] "Decrypt a value"))