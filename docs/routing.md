# Routing and Matrix

D-core provides a provider-neutral routing abstraction with two implementations:

- OSRM
- Valhalla

Scope in v1:

- route computation (`route`)
- duration/distance matrix computation (`matrix`)

Out of scope in this module:

- service-level multi-provider fallback orchestration
- provider cost strategy / regional policy
- ETA prediction beyond provider baseline routing durations

## Protocol

Namespace: `d-core.core.routing.protocol`

Protocol: `RoutingProtocol`

- `route [this request opts]`
- `matrix [this request opts]`

## Components

### OSRM router

Namespace: `d-core.core.routing.osrm`

Integrant key: `:d-core.core.routing.osrm/router`

Required config:

- `:http-client` (`:d-core.core.http/client`)

Common config:

- `:user-agent` (default `"d-core routing/osrm"`)

Implementation defaults:

- route endpoint: `/route/v1/{profile}/{lon,lat;...}`
- matrix endpoint: `/table/v1/{profile}/{lon,lat;...}`
- profile mapping: `:driving/:walking/:cycling`

### Valhalla router

Namespace: `d-core.core.routing.valhalla`

Integrant key: `:d-core.core.routing.valhalla/router`

Required config:

- `:http-client` (`:d-core.core.http/client`)

Common config:

- `:user-agent` (default `"d-core routing/valhalla"`)

Implementation defaults:

- route endpoint: `/route`
- matrix endpoint: `/sources_to_targets`
- profile mapping: `:driving -> auto`, `:walking -> pedestrian`, `:cycling -> bicycle`
- units normalized to meters in result envelopes

## Input and output shape

Schemas live in `d-core.core.routing.schema`.

### `route` request

Required:

- `:locations` (vector with at least 2 points)
  - point: `{:lat number :lon number}`

Optional provider-neutral fields:

- `:profile` (`:driving`, `:walking`, `:cycling`, default `:driving`)
- `:alternatives` (`boolean` or positive integer)
- `:steps` (`boolean`)
- `:overview` (`:full`, `:simplified`, `:none`, default `:simplified`)
- `:include-geometry` (`boolean`, default `true`)
- `:language` (`string`)
- `:units` (`:kilometers` or `:miles`, default `:kilometers`)

### `matrix` request

Required:

- `:sources` (vector of points, min 1)
- `:targets` (vector of points, min 1)

Optional provider-neutral fields:

- `:profile` (`:driving`, `:walking`, `:cycling`, default `:driving`)
- `:annotations` (subset of `[:duration :distance]`, default both)
- `:language` (`string`)
- `:units` (`:kilometers` or `:miles`, default `:kilometers`)
- `:verbose` (`boolean`, default `false`)

### `route` result envelope

```clojure
{:routes [{:distance number
           :duration number
           :geometry {:format keyword
                      :value (or string map vector)}
           :legs [{:distance number
                   :duration number
                   :summary string?}]
           :provider {:name keyword
                      :route-id string?
                      :raw map?}
           :metadata map?}]
 :provider keyword
 :metadata map?}
```

### `matrix` result envelope

```clojure
{:durations [[number-or-nil ...] ...] ; optional, present when requested
 :distances [[number-or-nil ...] ...] ; optional, present when requested
 :sources [{:location {:lat number :lon number}
            :name string?
            :provider map?
            :metadata map?}]
 :targets [{:location {:lat number :lon number}
            :name string?
            :provider map?
            :metadata map?}]
 :provider keyword
 :metadata map?}
```

Behavior:

- no route is normalized as `{:routes [] ...}`
- no matrix path is normalized with `nil` cells in requested matrices
- transport errors, malformed provider payloads, and schema violations throw `ex-info`
- provider-specific request knobs stay in `opts`

## Config examples

### Local OSRM

```edn
{:system
 {:d-core.core.http/client
  {:id :osrm-local
   :base-url "http://localhost:5001"
   :default-headers {"Accept" "application/json"}
   :http-opts {:socket-timeout 15000
               :conn-timeout 15000}}

  :d-core.core.routing.osrm/router
  {:http-client #ig/ref :d-core.core.http/client
   :user-agent "my-app routing/osrm"}}}
```

### Local Valhalla

```edn
{:system
 {:d-core.core.http/client
  {:id :valhalla-local
   :base-url "http://localhost:8002"
   :default-headers {"Accept" "application/json"}
   :http-opts {:socket-timeout 20000
               :conn-timeout 20000}}

  :d-core.core.routing.valhalla/router
  {:http-client #ig/ref :d-core.core.http/client
   :user-agent "my-app routing/valhalla"}}}
```

## Local docker-compose services

`docker-compose.yaml` includes:

- `osrm` on `localhost:5001`
- `valhalla` on `localhost:8002`
- `tileserver` on `localhost:8089` (serves local MBTiles for map visualization)

OSRM env vars:

- `DCORE_OSRM_PBF_URL` (default Monaco extract)
- `DCORE_OSRM_PROFILE` (default `car`)
- `DCORE_OSRM_ALGORITHM` (default `mld`)

Valhalla env vars:

- `DCORE_VALHALLA_PBF_URL` (default Monaco extract)
- `DCORE_VALHALLA_THREADS` (default `1`)
- `DCORE_VALHALLA_BUILD_ELEVATION` (default `False`)
- `DCORE_VALHALLA_BUILD_ADMINS` (default `True`)
- `DCORE_VALHALLA_BUILD_TIME_ZONES` (default `True`)

Tile env vars:

- `DCORE_TILESERVER_MBTILES` (default `monaco.mbtiles`)
- `DCORE_TILEBUILDER_AREA` (default `monaco`)
- `DCORE_TILEBUILDER_JAVA_TOOL_OPTIONS` (default `-Xmx2g`)
- `DCORE_TILEBUILDER_MINZOOM` (default `0`)
- `DCORE_TILEBUILDER_MAXZOOM` (default `12`)
- `DCORE_TILEBUILDER_RENDER_MAXZOOM` (default `12`)

Build tiles once (on demand profile):

```bash
docker compose --profile tiles-build run --rm tilebuilder
```

Start tile server:

```bash
docker compose up -d tileserver
```

## Integration tests

Integration test namespace:

- `d-core.integration.routing-engines-test`

Environment gates:

- `INTEGRATION=1` or `DCORE_INTEGRATION=1` or `DCORE_INTEGRATION_ROUTING=1`
- optional `DCORE_OSRM_URL` (default `http://localhost:5001`)
- optional `DCORE_VALHALLA_URL` (default `http://localhost:8002`)

Example:

```bash
INTEGRATION=1 clojure -M:test -n d-core.integration.routing-engines-test
```
