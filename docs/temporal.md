# Temporal

D-Core currently exposes Temporal at the client boundary only. It does not define workflow classes, activity classes, workers, task routing, or a D-Core job runtime on top of Temporal.

This is intentional: Temporal is a durable workflow orchestration system, not just another queue backend. It is a good fit for long-running processes, retries with history, distributed task execution, timers, human-in-the-loop steps, sagas, and workflows that must survive process restarts. It is usually too much for simple fire-and-forget pub/sub or short message fanout, where the existing producer/consumer abstractions are lighter.

## Dependency

The Java SDK is included through `io.temporal/temporal-sdk`.

## Integrant

```edn
{:d-core.core.clients.temporal/client
 {:target "localhost:7233"
  :namespace "default"}}
```

By default the client points at the local Temporal development service on `localhost:7233` and namespace `default`.

Useful options:

- `:target` gRPC target, default `"localhost:7233"`
- `:namespace` Temporal namespace, default `"default"`
- `:connect?` when true, connects during init instead of waiting for first use
- `:connect-timeout-ms` connect timeout when `:connect?` is true
- `:enable-https?` enables TLS on the service stubs
- `:headers` gRPC headers map
- `:rpc-timeout-ms`, `:rpc-long-poll-timeout-ms`, `:rpc-query-timeout-ms`
- `:configure-service-options` function that receives the Java `WorkflowServiceStubsOptions$Builder`
- `:configure-client-options` function that receives the Java `WorkflowClientOptions$Builder`

## Low-Level Use

```clojure
(require '[d-core.core.clients.temporal.client :as temporal])

(def client
  (temporal/make-client {:target "localhost:7233"
                         :namespace "default"}))

(temporal/health client)

(temporal/start-untyped-workflow!
 client
 "MyWorkflow"
 {:task-queue "jobs"
  :workflow-id "job-123"}
 {:job-id "job-123"})

(temporal/close! client)
```

## Local Development Server

D-Core's `docker-compose.yaml` includes a Temporal development service backed by a local SQLite file in the `temporal_data` Docker volume.

```bash
docker compose up temporal
```

The Temporal service listens on `localhost:7233`; the Web UI is available on `http://localhost:8233`.

You can also start Temporal directly with the Temporal CLI:

```bash
temporal server start-dev
```

## Integration Test

```bash
INTEGRATION=1 clojure -M:test -n d-core.integration.temporal-test
```

Optional environment variables:

- `DCORE_TEMPORAL_TARGET`, default `"localhost:7233"`
- `DCORE_TEMPORAL_NAMESPACE`, default `"default"`

## Current Boundary

Use this integration when an application already knows its Temporal workflow type, task queue, workflow id, and arguments. The next layer should decide how D-Core domain jobs map to Temporal workflow/activity definitions and worker lifecycle.
