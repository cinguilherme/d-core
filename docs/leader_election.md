# Leader Election

`d-core` provides a lease-based leader election abstraction for cases where many
instances may be running, but only one should actively perform a piece of work.

v1 includes:
- a portable `LeaderElectionProtocol`
- Redis-backed leader election (`:d-core.core.leader-election.redis/redis`)
- Valkey-backed leader election (`:d-core.core.leader-election.valkey/valkey`)
- Postgres-backed leader election (`:d-core.core.leader-election.postgres/postgres`)
- Kubernetes Lease-backed leader election (`:d-core.core.leader-election.kubernetes-lease/kubernetes-lease`)

v1 intentionally does **not** include:
- Quartz coupling
- a scheduler/runtime helper that auto-renews in the background
- ZooKeeper backends

## Protocol

Namespace:

```clj
(require '[d-core.core.leader-election.protocol :as leader])
```

Methods:

```clj
(leader/acquire! election election-id opts)
(leader/renew! election election-id token opts)
(leader/resign! election election-id token opts)
(leader/status election election-id opts)
```

Portable options:
- `:lease-ms` overrides the component default lease duration for `acquire!` and `renew!`

Portable statuses:
- `acquire!`: `:acquired` or `:busy`
- `renew!`: `:renewed` or `:lost`
- `resign!`: `:released` or `:not-owner`
- `status`: `:vacant` or `:held`

Portable result fields:
- always: `:ok`, `:status`, `:backend`, `:election-id`
- holder metadata when known: `:owner-id`, `:fencing`, `:remaining-ttl-ms`
- only on successful `acquire!`/`renew!`: `:token`

`status` never returns the current leader token.

## Config

### Redis

```edn
{:system
 {:d-core.core.clients.redis/client {:uri "redis://localhost:6379"}
  :d-core.core.leader-election.redis/redis
  {:redis-client #ig/ref :d-core.core.clients.redis/client
   :owner-id "orders-worker-1"
   :prefix "dcore:leader-election:"
   :default-lease-ms 15000}}}
```

### Valkey

```edn
{:system
 {:d-core.core.clients.valkey/client {:uri "redis://localhost:6380"}
  :d-core.core.leader-election.valkey/valkey
  {:valkey-client #ig/ref :d-core.core.clients.valkey/client
   :owner-id "orders-worker-1"
   :prefix "dcore:leader-election:"
   :default-lease-ms 15000}}}
```

### Postgres

```edn
{:system
 {:d-core.core.clients.postgres/client
  {:jdbc-url "jdbc:postgresql://localhost:5432/core-service"
   :username "postgres"
   :password "postgres"
   :pool? true}

  :d-core.core.leader-election.postgres/postgres
  {:postgres-client #ig/ref :d-core.core.clients.postgres/client
   :owner-id "orders-worker-1"
   :bootstrap-schema? true
   :table-name "dcore_leader_elections"
   :default-lease-ms 15000}}}
```

### Kubernetes Lease

This backend is **in-cluster only**. It expects to run in a Pod with a mounted
ServiceAccount token and CA bundle, and it talks directly to the Kubernetes API
server.

```edn
{:system
 {:d-core.core.clients.kubernetes/client
  {:namespace "workers"}

  :d-core.core.leader-election.kubernetes-lease/kubernetes-lease
  {:kubernetes-client #ig/ref :d-core.core.clients.kubernetes/client
   :owner-id "orders-worker-1"
   :lease-name-prefix "dcore-leader-"
   :default-lease-ms 15000}}}
```

Component options:
- `:redis-client`, `:valkey-client`, `:postgres-client`, or `:kubernetes-client` is required depending on backend
- `:owner-id` defaults to `<hostname>:<uuid>`; production systems should set this explicitly
- `:default-lease-ms` defaults to `15000`
- `:clock` may be a `java.time.Clock`, a `d-core.libs.time/clock` component, a clock opts map, or a function returning epoch ms / `Instant`
- Redis/Valkey only: `:prefix` defaults to `"dcore:leader-election:"`
- Postgres only: `:table-name` defaults to `"dcore_leader_elections"`
- Postgres only: `:bootstrap-schema?` defaults to `false`
- Postgres lease expiry is evaluated from database time; `:clock` is not authoritative for Postgres lease validity
- Kubernetes Lease only: `:lease-name-prefix` defaults to `"dcore-leader-"`
- Kubernetes Lease only: `:lease-name-prefix` is validated strictly at init against the backend's actual Lease-name generation, including the fallback hashed form used when no election-id characters fit
- Kubernetes client only: `:api-server-url` defaults from `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT_HTTPS`
- Kubernetes client only: `:token-file`, `:ca-cert-file`, and `:namespace-file` default to the mounted ServiceAccount paths under `/var/run/secrets/kubernetes.io/serviceaccount/`
- Kubernetes client only: `:namespace` may override the mounted namespace file
- Kubernetes client only: `:request-timeout-ms` defaults to `5000`

## Kubernetes RBAC

The Kubernetes backend needs `get`, `create`, and `update` on `leases` in the
workload namespace.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: dcore-leader-election
  namespace: workers
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: dcore-leader-election
  namespace: workers
subjects:
  - kind: ServiceAccount
    name: orders-worker
    namespace: workers
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: dcore-leader-election
```

## Usage

```clj
(require '[d-core.core.leader-election.protocol :as leader])

(let [attempt (leader/acquire! election :orders-sync {:lease-ms 5000})]
  (case (:status attempt)
    :acquired
    (try
      ;; run singleton work here
      (leader/renew! election :orders-sync (:token attempt) {:lease-ms 5000})
      (finally
        (leader/resign! election :orders-sync (:token attempt) nil)))

    :busy
    {:leader (:owner-id attempt)
     :retry-after-ms (:remaining-ttl-ms attempt)}))
```

## Semantics And Limits

- Redis and Valkey use a single lease key per election and a separate monotonic fencing counter.
- Postgres uses a single persistent row per election and preserves fencing across release/reacquire cycles.
- Kubernetes Lease uses one namespaced `coordination.k8s.io/v1` `Lease` object per election.
- Redis, Valkey, and Postgres all evaluate lease expiry authoritatively in the backend they coordinate through.
- Kubernetes Lease expiry is evaluated from the locally observed `renewTime + leaseDurationSeconds`, so local clock quality matters for that backend.
- Kubernetes Lease liveness and takeover decisions are based on Lease spec fields (`holderIdentity`, `renewTime`, and `leaseDurationSeconds`), not on the private token annotation.
- Very long Kubernetes Lease prefixes may consume the visible election-id segment entirely; the backend still generates a valid prefix-plus-hash Lease name within Kubernetes' 63-character limit.
- Fencing increments only on fresh acquisition, not on renewals.
- Renew and release are token-checked, so one holder cannot accidentally release another holder’s lease after expiry.
- For Kubernetes Lease specifically, the private token annotation is only used to prove ownership for `renew!` and `resign!`. If the annotation is missing but the Lease spec is still active, `status` reports `:held` and competing instances treat it as `:busy`.
- The Postgres backend preserves the lease contract with a table-backed design using database time. It does **not** use advisory locks.
- The Redis/Valkey backends are single-endpoint lease coordination. They are **not** Redlock or multi-master quorum consensus.
- The Postgres backend works with existing pooled or unpooled `postgres-client` datasources.
- The Kubernetes backend is **best-effort**, not strong fencing. Its `:fencing` value maps to `spec.leaseTransitions`, which is useful as a monotonic acquisition counter but not equivalent to database-authoritative fencing.
- The Kubernetes backend is in-cluster only in this version. It does **not** support kubeconfig loading, exec auth plugins, or out-of-cluster auth flows.
- Kubernetes Lease duration is second-granularity. `d-core` still accepts `:lease-ms`, but the backend rounds up to whole seconds with a minimum of `1`.
- The Kubernetes backend requires a modern supported cluster with the `coordination.k8s.io/v1` `Lease` API available. It does **not** depend on the newer coordinated leader election `LeaseCandidate` API.
- Kubernetes works on both on-demand and spot/preemptible nodes. On spot capacity, leader loss is a routine path; pick lease durations that balance failover time against false leadership loss during API hiccups or GC pauses.
- For real HA on Kubernetes, run at least two replicas, spread them across nodes or zones when possible, and keep singleton side effects idempotent.
- Quartz JDBC clustering remains a Quartz-specific coordination mechanism and is not reused by this abstraction.
