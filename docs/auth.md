# Authentication and Authorization

D-core provides a small, protocol-driven surface for authentication (Authn) and
authorization (Authz). The default implementation targets JWT access tokens
issued by OIDC/OAuth2 providers like Keycloak.

## Concepts

- Authentication verifies a token and returns a normalized principal map.
- Authorization checks tenant and scopes against that principal.
- Middleware wires these into Ring handlers.

## Protocols

- `d-core.core.authn.protocol/Authenticator`
- `d-core.core.authz.protocol/Authorizer`

## Default implementations

### JWT Authenticator

Namespace: `d-core.core.authn.jwt`

Integrant key: `:d-core.core.authn.jwt/authenticator`

Required config:

- `:jwks-uri` or `:jwks` (static JWKS map)

Common config:

- `:issuer` (OIDC issuer URL)
- `:aud` (audience string)
- `:jwks-cache-ttl-ms` (default 300000)
- `:clock-skew-ms` (default 60000)
- `:tenant-claim` (default "tenant_id")
- `:scope-claim` (default "scope")
- `:http-opts` (passed to `clj-http`)

Principal shape:

- `:subject`
- `:tenant-id`
- `:scopes` (set of strings)
- `:client-id`
- `:aud`
- `:issuer`
- `:expires-at`
- `:actor`
- `:claims` (raw claims map)

### API Key Authenticator

Namespace: `d-core.core.authn.api-key`

Integrant key: `:d-core.core.authn.api-key/authenticator`

Required config:

- `:api-key-store`

Reads credentials from:

- `x-api-key`
- `Authorization: ApiKey <token>`
- `Authorization: Api-Key <token>`

Principal shape includes:

- `:subject` (api key id)
- `:tenant-id`
- `:scopes`
- `:auth-type` (`:api-key`)
- `:api-key/id`
- `:api-key/limits`
- `:api-key/metadata`

### Authenticator Chain

Namespace: `d-core.core.authn.chain`

Integrant key: `:d-core.core.authn.chain/authenticator`

Use this when endpoints should accept multiple credential types (for example
JWT and API keys) in priority order.

### Scope Authorizer

Namespace: `d-core.core.authz.scope`

Integrant key: `:d-core.core.authz.scope/authorizer`

Request context:

- `:tenant` (string/keyword)
- `:scopes` (string, coll, or set)

## HTTP middleware

Namespace: `d-core.core.auth.http`

- `wrap-authentication`: verifies a bearer token and attaches
  `:auth/principal` and `:auth/token` to the request.
- `wrap-authorization`: enforces `:auth/require` from the request or a
  `require-fn` option.

Auth requirement shape:

```clojure
{:tenant "tenant-1"
 :scopes #{"messages:read" "messages:write"}}
```

Integrant keys:

- `:d-core.core.auth.http/authentication-middleware`
- `:d-core.core.auth.http/authorization-middleware`

### API key limitations middleware

Namespace: `d-core.core.auth.api-key`

Integrant key: `:d-core.core.auth.api-key/limitations-middleware`

Applies per-key constraints for API key principals:

- fixed-window rate limit (`:rate-limit {:limit ... :window-ms ...}`)
- method allowlist
- path allowlist
- IP allowlist/denylist
- limiter failure policy (`:rate-limit-fail-open?` in middleware opts)

This middleware uses an injected `RateLimitProtocol` backend (for example
` :d-core.core.rate-limit.redis/limiter`).

## Token client (service-to-service)

Namespace: `d-core.core.auth.token-client`

Integrant key: `:d-core.core.auth/token-client`

Supports:

- `client-credentials`
- `token-exchange` (RFC 8693)

Required config:

- `:token-url`

Common config:

- `:client-id`
- `:client-secret`
- `:http-opts` (passed to `clj-http`)

## Minimal config example

```clojure
{:d-core.core.authn.jwt/authenticator
 {:issuer "https://auth.example.com/realms/dev"
  :aud "d-core-api"
  :jwks-uri "https://auth.example.com/realms/dev/protocol/openid-connect/certs"
  :tenant-claim "tenant_id"
  :scope-claim "scope"}

 :d-core.core.authn.api-key/authenticator
 {:api-key-store #ig/ref :d-core.core.api-keys.postgres/store}

 :d-core.core.authn.chain/authenticator
 {:authenticators [#ig/ref :d-core.core.authn.jwt/authenticator
                   #ig/ref :d-core.core.authn.api-key/authenticator]}

 :d-core.core.authz.scope/authorizer {}

 :d-core.core.auth.http/authentication-middleware
 {:authenticator #ig/ref :d-core.core.authn.chain/authenticator}

 :d-core.core.auth.http/authorization-middleware
 {:authorizer #ig/ref :d-core.core.authz.scope/authorizer}

 :d-core.core.clients.redis/client
 {:uri "redis://localhost:6379"}

 :d-core.core.rate-limit.redis/limiter
 {:redis-client #ig/ref :d-core.core.clients.redis/client
  :prefix "dcore:rate-limit:"
  :limit 100
  :window-ms 60000}

 :d-core.core.auth.api-key/limitations-middleware
 {:rate-limiter #ig/ref :d-core.core.rate-limit.redis/limiter
  :opts {:rate-limit-fail-open? false}}}
```
