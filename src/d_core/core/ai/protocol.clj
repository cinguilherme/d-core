(ns d-core.core.ai.protocol)

(defprotocol GenerationProtocol
  "Provider-neutral generation contract.

  `request` is a plain map validated by `d-core.core.ai.schema`.

  `opts` is reserved for transport or provider-specific request controls that
  should not leak into the shared input contract."
  (generate [this request opts]
    "Generate text or structured output from text/vision inputs."))

(defprotocol ModelCapabilitiesProtocol
  "Provider-neutral capabilities contract."
  (capabilities [this opts]
    "Return normalized capability metadata for the configured provider."))
