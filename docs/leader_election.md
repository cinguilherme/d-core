# Leader Election

`d-core` provides a lease-based leader election abstraction for cases where many
instances may be running, but only one should actively perform a piece of work.

v1 includes:
- a portable `LeaderElectionProtocol`
- Redis-backed leader election (`:d-core.core.leader-election.redis/redis`)
- Valkey-backed leader election (`:d-core.core.leader-election.valkey/valkey`)

v1 intentionally does **not** include:
- Quartz coupling
- a scheduler/runtime helper that auto-renews in the background
- JDBC/Postgres, Kubernetes Lease, or ZooKeeper backends

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

Component options:
- `:redis-client` or `:valkey-client` is required
- `:owner-id` defaults to `<hostname>:<uuid>`; production systems should set this explicitly
- `:prefix` defaults to `"dcore:leader-election:"`
- `:default-lease-ms` defaults to `15000`
- `:clock` may be a `java.time.Clock`, a `d-core.libs.time/clock` component, a clock opts map, or a function returning epoch ms / `Instant`

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
- Fencing increments only on fresh acquisition, not on renewals.
- Renew and release are token-checked, so one holder cannot accidentally release another holder’s lease after expiry.
- This is single-endpoint lease coordination. It is **not** Redlock or multi-master quorum consensus.
- Quartz JDBC clustering remains a Quartz-specific coordination mechanism and is not reused by this abstraction.
