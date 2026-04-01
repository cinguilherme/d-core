# Geocoding

D-core provides a provider-neutral geocoding abstraction with a first
Nominatim-backed implementation.

Scope in v1:

- forward geocoding (`geocode`)
- reverse geocoding (`reverse-geocode`)

Out of scope in this module:

- routing / ETA / distance matrix
- autocomplete / place search UX concerns
- service-level provider orchestration policy

## Protocol

Namespace: `d-core.core.geocoding.protocol`

Protocol: `GeocodingProtocol`

- `geocode [this query opts]`
- `reverse-geocode [this location opts]`

## Components

### Nominatim geocoder

Namespace: `d-core.core.geocoding.nominatim`

Integrant key: `:d-core.core.geocoding.nominatim/geocoder`

Required config:

- `:http-client` (`:d-core.core.http/client`)

Common config:

- `:user-agent` (default `"d-core geocoder"`)
- `:email` (optional, forwarded as `email` query parameter)

Implementation defaults:

- `search` and `reverse` endpoints
- `format=geocodejson`
- `addressdetails=1`

### Cached geocoder wrapper

Namespace: `d-core.core.geocoding.cached`

Integrant key: `:d-core.core.geocoding.cached/geocoder`

Required config:

- `:geocoder` (any `GeocodingProtocol` impl)
- `:cache` (any `CacheProtocol` impl)

Optional config:

- `:id` cache namespace seed (default `:geocoder`)
- `:hit-ttl-ms` (default 30 days)
- `:empty-ttl-ms` (default 1 hour)

Behavior:

- caches normalized envelopes
- deterministic keys based on operation + normalized input
- caches successful non-empty and empty results with different TTLs
- does not cache exceptions

## Input and output shape

Schemas live in `d-core.core.geocoding.schema`.

`geocode` accepts either:

- `{:text "..."}`
- `{:address {:house-number ... :street ... :city ... ...}}`

Provider-neutral optional fields:

- `:limit`
- `:language`
- `:country-codes`

`reverse-geocode` accepts:

- `{:lat <number> :lon <number>}`
- optional `:language`

Both methods return:

```clojure
{:items [{:formatted-address string?
          :location {:lat number :lon number}
          :components {...}
          :bounds {...}
          :provider {:name :nominatim
                     :place-id string?
                     :osm-type string?
                     :osm-id string?
                     :licence string?
                     :raw map?}
          :metadata map?}]
 :provider :nominatim
 :metadata map?}
```

No-result behavior:

- geocode/reverse normalize to `{:items [] ...}` instead of throwing.

## Config examples

### Local Nominatim (docker-compose)

```edn
{:system
 {:d-core.core.http/client
  {:id :nominatim-local
   :base-url "http://localhost:8088"
   :default-headers {"Accept" "application/json"}
   :http-opts {:socket-timeout 10000
               :conn-timeout 10000}}

  :d-core.core.geocoding.nominatim/geocoder
  {:http-client #ig/ref :d-core.core.http/client
   :user-agent "my-app geocoding/local"}}}
```

### Public Nominatim (conservative)

```edn
{:system
 {:d-core.core.http/client
  {:id :nominatim-public
   :base-url "https://nominatim.openstreetmap.org"
   :default-headers {"Accept" "application/json"}
   :policies {:rate-limit {:rate-per-sec 1
                           :burst 1}
              :bulkhead {:max-concurrent 1}
              :retry {:max-attempts 2}}
   :http-opts {:socket-timeout 15000
               :conn-timeout 15000}}

  :d-core.core.geocoding.nominatim/geocoder
  {:http-client #ig/ref :d-core.core.http/client
   :user-agent "my-app geocoding/public"
   :email "ops@example.com"}

  :d-core.core.cache.in-memory/in-memory {:logger #ig/ref :duct/logger}

  :d-core.core.geocoding.cached/geocoder
  {:id :nominatim
   :geocoder #ig/ref :d-core.core.geocoding.nominatim/geocoder
   :cache #ig/ref :d-core.core.cache.in-memory/in-memory
   :hit-ttl-ms 2592000000
   :empty-ttl-ms 3600000}}}
```

## Local docker-compose service

`docker-compose.yaml` includes a `nominatim` service on `localhost:8088`.

Configurable env vars:

- `DCORE_NOMINATIM_PBF_URL` (defaults to Monaco extract)
- `DCORE_NOMINATIM_REPLICATION_URL` (defaults to Monaco updates URL)
- `DCORE_NOMINATIM_UPDATE_MODE` (default `none`)
- `DCORE_NOMINATIM_FREEZE` (default `true`)
- `DCORE_NOMINATIM_IMPORT_STYLE` (default `address`)
- `DCORE_NOMINATIM_THREADS` (default `1`)
- `DCORE_NOMINATIM_POSTGRES_SHARED_BUFFERS` (default `256MB`)
- `DCORE_NOMINATIM_POSTGRES_MAINTENANCE_WORK_MEM` (default `512MB`)
- `DCORE_NOMINATIM_POSTGRES_AUTOVACUUM_WORK_MEM` (default `128MB`)
- `DCORE_NOMINATIM_POSTGRES_EFFECTIVE_CACHE_SIZE` (default `1024MB`)
- `DCORE_NOMINATIM_PASSWORD`

The default Monaco extract keeps import size and startup time small for local
development. Use `DCORE_NOMINATIM_PBF_URL` to swap to another region when
needed.

The compose defaults intentionally favor lower-memory imports for local
developer machines (including Apple Silicon). If your Docker VM has more RAM
available and you want faster imports, raise the Postgres memory values and
`DCORE_NOMINATIM_THREADS`.

## Integration tests

Integration test namespace:

- `d-core.integration.geocoding-nominatim-test`

Environment gates:

- `INTEGRATION=1` or `DCORE_INTEGRATION=1` or `DCORE_INTEGRATION_GEOCODING=1`
- optional `DCORE_NOMINATIM_URL` (default `http://localhost:8088`)

Example:

```bash
INTEGRATION=1 clojure -M:test -n d-core.integration.geocoding-nominatim-test
```
