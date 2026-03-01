# API Keys

D-core provides a protocol-first API key subsystem with a Postgres backend.
It covers key lifecycle management, authentication, and per-key limitations
including fixed-window rate limiting.

## Protocol

- `d-core.core.api-keys.protocol/ApiKeyStore`

Core operations:

- `ensure-schema!`
- `create-key!`
- `get-key`
- `list-keys`
- `revoke-key!`
- `rotate-key!`
- `authenticate-key`
- `consume-rate-limit!`

## Postgres backend

Namespace: `d-core.core.api-keys.postgres`

Integrant key: `:d-core.core.api-keys.postgres/store`

Required config:

- `:postgres-client`

Common config:

- `:pepper` (recommended)
- `:bootstrap-schema?` (default `false`)

## API key authenticator

Namespace: `d-core.core.authn.api-key`

Integrant key: `:d-core.core.authn.api-key/authenticator`

Reads API keys from:

- `x-api-key`
- `Authorization: ApiKey <token>`
- `Authorization: Api-Key <token>`

## Optional authenticator chaining

Namespace: `d-core.core.authn.chain`

Integrant key: `:d-core.core.authn.chain/authenticator`

Use this when you want to accept both JWT and API keys in the same endpoint.

## API key limitations middleware

Namespace: `d-core.core.auth.api-key`

Integrant key: `:d-core.core.auth.api-key/limitations-middleware`

Built-in checks for API-key principals:

- `:method-allowlist`
- `:path-allowlist` (exact or prefix with `*`)
- `:ip-allowlist`
- `:ip-denylist`
- `:rate-limit {:limit n :window-ms m}`

## Example wiring

```clojure
{:d-core.core.clients.postgres/client
 {:jdbc-url "jdbc:postgresql://localhost:5432/d-core"
  :username "postgres"
  :password "postgres"}

 :d-core.core.api-keys.postgres/store
 {:postgres-client #ig/ref :d-core.core.clients.postgres/client
  :pepper "replace-in-production"
  :bootstrap-schema? true}

 :d-core.core.authn.jwt/authenticator
 {:issuer "https://auth.example.com/realms/dev"
  :aud "d-core-api"
  :jwks-uri "https://auth.example.com/realms/dev/protocol/openid-connect/certs"}

 :d-core.core.authn.api-key/authenticator
 {:api-key-store #ig/ref :d-core.core.api-keys.postgres/store}

 :d-core.core.authn.chain/authenticator
 {:authenticators [#ig/ref :d-core.core.authn.jwt/authenticator
                   #ig/ref :d-core.core.authn.api-key/authenticator]}

 :d-core.core.auth.http/authentication-middleware
 {:authenticator #ig/ref :d-core.core.authn.chain/authenticator}

 :d-core.core.authz.scope/authorizer {}

 :d-core.core.auth.http/authorization-middleware
 {:authorizer #ig/ref :d-core.core.authz.scope/authorizer}

 :d-core.core.auth.api-key/limitations-middleware
 {:api-key-store #ig/ref :d-core.core.api-keys.postgres/store}}
```
