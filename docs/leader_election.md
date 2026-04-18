# Leader Election

`d-core` provides a lease-based leader election abstraction for cases where many
instances may be running, but only one should actively perform a piece of work.

v1 includes:
- a portable `LeaderElectionProtocol`
- Redis-backed leader election (`:d-core.core.leader-election.redis/redis`)
- Valkey-backed leader election (`:d-core.core.leader-election.valkey/valkey`)
- Postgres-backed leader election (`:d-core.core.leader-election.postgres/postgres`)

v1 intentionally does **not** include:
- Quartz coupling
- a scheduler/runtime helper that auto-renews in the background
- Kubernetes Lease or ZooKeeper backends

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

Component options:
- `:redis-client`, `:valkey-client`, or `:postgres-client` is required depending on backend
- `:owner-id` defaults to `<hostname>:<uuid>`; production systems should set this explicitly
- `:default-lease-ms` defaults to `15000`
- `:clock` may be a `java.time.Clock`, a `d-core.libs.time/clock` component, a clock opts map, or a function returning epoch ms / `Instant`
- Redis/Valkey only: `:prefix` defaults to `"dcore:leader-election:"`
- Postgres only: `:table-name` defaults to `"dcore_leader_elections"`
- Postgres only: `:bootstrap-schema?` defaults to `false`

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
- Fencing increments only on fresh acquisition, not on renewals.
- Renew and release are token-checked, so one holder cannot accidentally release another holder’s lease after expiry.
- The Postgres backend preserves the lease contract with a table-backed design. It does **not** use advisory locks.
- The Redis/Valkey backends are single-endpoint lease coordination. They are **not** Redlock or multi-master quorum consensus.
- The Postgres backend works with existing pooled or unpooled `postgres-client` datasources.
- Quartz JDBC clustering remains a Quartz-specific coordination mechanism and is not reused by this abstraction.
