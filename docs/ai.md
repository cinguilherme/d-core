# AI Generation

D-core provides a provider-neutral AI generation abstraction with an
LM Studio OpenAI-compatible implementation.

Scope in v1:

- text generation (`generate`)
- structured JSON output (`generate` with `:output {:type :json-schema ...}`)
- vision input (`image->text`) via message content parts
- provider capability lookup (`capabilities`)

Out of scope in this module:

- image generation
- streaming generation
- tool/function-calling normalization
- service-level multi-provider fallback/routing policy

## Protocol

Namespace: `d-core.core.ai.protocol`

Protocols:

- `GenerationProtocol`
  - `generate [this request opts]`
- `ModelCapabilitiesProtocol`
  - `capabilities [this opts]`

## Components

### LM Studio OpenAI-compatible provider

Namespace: `d-core.core.ai.lm-studio.openai`

Integrant key: `:d-core.core.ai.lm-studio.openai/provider`

Required config:

- `:http-client` (`:d-core.core.http/client`)

Optional config:

- `:default-model` (used when request does not include `:model`)
- `:capabilities` (overrides defaults returned by `capabilities`)
- `:user-agent` (default `"d-core ai/lm-studio-openai"`)

Implementation defaults:

- endpoint: `POST /chat/completions`
- non-streaming requests
- OpenAI-style chat content mapping for text and images

### Common provider delegator

Namespace: `d-core.core.ai.common`

Integrant key: `:d-core.core.ai/common`

Required config:

- `:providers` map of provider-id to provider instance

Optional config:

- `:default-provider` (default `:lm-studio-openai`)
- `:logger`

Behavior:

- selects provider by `opts :provider`, falling back to `:default-provider`
- delegates both `generate` and `capabilities`
- no fallback/orchestration beyond single-provider selection

## Input and output shape

Schemas live in `d-core.core.ai.schema`.

### `generate` request

Required:

- `:messages` (vector, min 1)

Message fields:

- `:role` (`:system`, `:developer`, `:user`, `:assistant`, `:tool`)
- `:content` (vector of content parts, min 1)

Supported content parts:

- text: `{:type :text :text "..."}`
- image URL: `{:type :image-url :url "https://..."}`
- image base64: `{:type :image-base64 :data "..." :media-type "image/png"}`

Optional provider-neutral fields:

- `:model` (string)
- `:output`
  - `{:type :text}` (default)
  - `{:type :json-schema :schema <map> :strict? true|false :name "..."}` 
- `:sampling` (`:temperature`, `:top-p`, `:presence-penalty`,
  `:frequency-penalty`, `:seed`)
- `:limits` (`:max-output-tokens`, `:timeout-ms`)
- `:metadata` (map)

### `generate` result envelope

```clojure
{:items [{:text string?
          :structured map?
          :finish-reason keyword?
          :index int?
          :role keyword?
          :provider map?
          :metadata map?}]
 :provider :lm-studio-openai
 :usage {:input-tokens int?
         :output-tokens int?
         :total-tokens int?}
 :metadata map?}
```

Structured-output behavior:

- when `:output {:type :json-schema ...}` is requested, assistant text must be
  valid JSON object content
- v1 enforces JSON-object parsing, not full semantic JSON Schema validation

### `capabilities` envelope

```clojure
{:provider keyword
 :input-modalities #{:text :image ...}
 :output-types #{:text :json-schema ...}
 :streaming? boolean
 :structured-output? boolean
 :vision? boolean
 :models [{:id string
           :input-modalities #{...}
           :output-types #{...}
           :structured-output? boolean?
           :vision? boolean?}]
 :metadata map?}
```

## Error normalization

Provider/transport failures normalize to typed categories:

- `:d-core.ai/unsupported-capability`
- `:d-core.ai/model-not-found`
- `:d-core.ai/context-too-large`
- `:d-core.ai/rate-limited`
- `:d-core.ai/provider-error`

## Config example (LM Studio external server)

```edn
{:system
 {:d-core.core.http/client
  {:id :lm-studio
   :base-url "http://localhost:1234/v1"
   :default-headers {"Accept" "application/json"}
   :http-opts {:socket-timeout 60000
               :conn-timeout 60000}
   :policies {:retry {:max-attempts 2}
              :bulkhead {:max-concurrent 16}}}

  :d-core.core.ai.lm-studio.openai/provider
  {:http-client #ig/ref :d-core.core.http/client
   :default-model "qwen2.5-7b-instruct"}

  :d-core.core.ai/common
  {:default-provider :lm-studio-openai
   :providers {:lm-studio-openai #ig/ref :d-core.core.ai.lm-studio.openai/provider}}}}
```

## Testing strategy

v1 intentionally does not include model-dependent automated integration tests.

Coverage includes:

- strict schema/unit tests
- adapter tests with mocked HTTP responses
- common delegator tests

## Dev playground

For live experiments against a running LM Studio server:

- namespace: `d-core.dev.lm-studio-playground`
- smoke helpers:
  - `smoke-text!`
  - `smoke-structured!`
  - `smoke-vision-url!`
  - `smoke-vision-base64!`
  - `show-capabilities`

Environment variables:

- `DCORE_LM_STUDIO_URL` (default `http://localhost:1234/v1`)
- `DCORE_LM_STUDIO_MODEL` (default `qwen2.5-7b-instruct`)
