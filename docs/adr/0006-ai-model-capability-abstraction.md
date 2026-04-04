# ADR 0006: AI Model Capability Abstraction v1 (LM Studio OpenAI-First)

Date: 2026-04-04
Status: Proposed

## Context

D-core is protocol-first infrastructure. It should expose reusable AI
capabilities without coupling consuming services to vendor-specific APIs or
wire formats.

AI integration has high variation:

- modality differences (`text->text`, `image->text`, `text->image`)
- output contract differences (free-form text vs structured output)
- transport and payload differences across providers
- local runtime differences (LM Studio can expose OpenAI-like and
  non-OpenAI-like behaviors depending on model/runtime configuration)

D-core responsibility remains infra abstraction. Service/application prompt
design, orchestration policy, and product behavior are not D-core concerns.

## Decision

Introduce AI integration as a capability-scoped abstraction with normalized
contracts and provider adapters.

### 1) Protocol surface (small and generic)

Define dedicated protocols under `d-core.core.ai`:

- `d-core.core.ai.protocol/GenerationProtocol`
  - `generate [this request opts]`
- `d-core.core.ai.protocol/ModelCapabilitiesProtocol`
  - `capabilities [this opts]`

The API remains map/primitive based, with `opts` as the provider-specific
escape hatch (same D-core rule used in other capabilities).

### 2) Canonical request/response model

Use shared schemas for provider-neutral contracts:

- `generate` canonical request supports:
  - model selection (`:model`)
  - multi-part input (`:messages` with typed content parts such as `:text`,
    `:image-url`, `:image-base64`)
  - output contract (`:output {:type :text}` or
    `:output {:type :json-schema :schema ... :strict? ...}`)
  - inference controls (`:sampling`, `:limits`)
- `generate` canonical response envelope includes:
  - `:items` normalized generation items (`:text`, optional `:structured`,
    `:finish-reason`)
  - `:usage` normalized token/accounting fields when available
  - `:provider` and optional provider metadata/raw payload
  - `:metadata`

`completion`, `suggestion`, and prompt-template semantics are application-level
specializations of `generate`; D-core will not create separate infra protocols
for those concepts in v1.

### 3) Provider selection and composition

Add a common delegator component, similar to text-search:

- `:d-core.core.ai/common`
  - selects backend by `:provider` in `opts` or configured default
  - delegates protocol calls to provider adapters

No fallback, ranking, or multi-provider orchestration policy in D-core v1.
Those remain service-owned decisions.

### 4) LM Studio first, with compatibility modes

First provider target is LM Studio via the OpenAI-compatible endpoint:

- OpenAI-compatible adapter path (for OpenAI-style chat/completions endpoints)

### 5) Error normalization

Provider/transport errors normalize to typed categories (illustrative):

- `:d-core.ai/unsupported-capability`
- `:d-core.ai/model-not-found`
- `:d-core.ai/context-too-large`
- `:d-core.ai/rate-limited`
- `:d-core.ai/provider-error`

Raw provider payloads may be preserved in error data for diagnostics.

### 6) Transport and resilience

Adapters should use `d-core.core.http/client` and inherit its policy controls
(timeouts, retries, rate limit, bulkhead, circuit breaker).

### 7) Testing and local development

- D-core v1 will not include automated integration tests that require a running
  model process.
- Coverage relies on schema/unit tests and adapter tests with mocked HTTP calls.
- A dedicated dev playground namespace supports manual LM Studio smoke checks
  during development.

## Rationale

- Capability-scoped protocols preserve long-term extensibility while keeping v1
  simple.
- Normalized schemas protect services from provider churn.
- LM Studio-first unblocks local development and experimentation immediately.
- Keeping orchestration out of D-core preserves service ownership of
  product/cost/latency tradeoffs.

## Consequences

- Services integrate AI through one stable contract and swap backends via
  configuration.
- Some features will surface as explicit capability errors depending on model
  and runtime support.
- Providers can evolve independently as long as canonical contracts are
  preserved.

## Scope (v1)

- Protocols + canonical schemas for `generate` and `capabilities`.
- LM Studio OpenAI-compatible provider adapter and common delegator wiring.
- Text generation, structured output, and vision input (`image->text`) in
  `generate` where supported by model.

## Out of scope (v1)

- Streaming contracts
- Image generation contract (`generate-image`)
- Tool/function-calling standardization
- Embeddings and reranking protocols
- Multi-provider fallback/routing policy
- Prompt template/version management
- Safety/policy layer beyond provider-native controls

## Config sketch

Illustrative only:

```edn
{:system
 {:d-core.core.http/client.ai
  {:id :ai
   :base-url "http://localhost:1234/v1"
   :policies {:retry {:max-attempts 2}
              :bulkhead {:max-concurrent 16}}}

  :d-core.core.ai.lm-studio.openai/provider
  {:http-client #ig/ref :d-core.core.http/client.ai
   :default-model "qwen2.5-7b-instruct"}

  :d-core.core.ai/common
  {:default-provider :lm-studio-openai
   :providers {:lm-studio-openai #ig/ref :d-core.core.ai.lm-studio.openai/provider}}}}
```

## Next Steps

- Define `d-core.core.ai.schema` with strict canonical contracts.
- Implement LM Studio OpenAI-compatible adapter and `:d-core.core.ai/common`.
- Add schema/unit tests + adapter tests with mocked HTTP responses.
- Add docs page with usage examples and a dev playground smoke workflow.

## Open Questions

- Which additional capability fields should be runtime-probed vs static config?
- When to add native LM Studio variant adapter support after OpenAI-compatible
  baseline stabilizes?
