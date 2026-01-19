# Async Messaging

This document describes how to wire async messaging in Duct with explicit
publish targets and decoupled consumer subscriptions.

## Design goals

- Explicit publish targets (no implicit fanout).
- Consumers only start from `:subscriptions` (publish config never creates consumers).
- Multi-cluster friendly (multiple client instances per transport).
- Clear separation: `:topics` (schema/metadata) vs `:publish` vs `:subscriptions`.

## Routing shape

```edn
{:topics
 {:order-created {:schema {:canonical [:map {:closed true} [:id :uuid] [:amount :double]]}
                  :relationship :both}}

 :publish
 {:order-created {:targets [{:producer :kafka-primary
                              :kafka-topic "core.orders"}
                             {:producer :kafka-data
                              :kafka-topic "core.orders.data"}]}}

 :subscriptions
 {:orders-worker {:topic :order-created
                  :source :kafka
                  :client :kafka-primary
                  :handler :my-app.handlers/order-created
                  :options {:group-id "orders-core"}}
  :orders-data-worker {:topic :order-created
                       :source :kafka
                       :client :kafka-data
                       :handler :my-app.handlers/order-created-data
                       :options {:group-id "orders-data"}}}}
```

Notes:
- `:topics` is shared metadata (schema, naming hints, etc).
- `:publish` is only used by producers. Each target can include per-target
  overrides (e.g. `:kafka-topic`, `:stream`, `:subject`).
- `:subscriptions` is the only source of consumers.
- `:client` is the cluster key. `:producer` is accepted as an alias for
  backward-compatibility.

## Producer fanout

Producers use `:publish` targets. One produce call can fan out to multiple
targets:

```clojure
(producer/produce! producer {:id 1} {:topic :order-created})
```

Targets are resolved in order. If any target fails, the call fails and the
whole produce is retried by your caller.

## Consumer wiring

Consumer runtimes filter subscriptions by `:source` only. The client instance
is chosen by `:client` (or `:producer`):

```edn
{:subscriptions
 {:orders-worker {:topic :order-created
                  :source :kafka
                  :client :kafka-primary
                  :handler :my-app.handlers/order-created}}}
```

No subscription is started for publish-only topics unless explicitly listed in
`:subscriptions`.

## Multi-cluster clients

Clients are named by the application developer. Example Kafka clients:

```edn
:d-core.core.clients/kafka
{:kafka-primary {:bootstrap-servers "localhost:29092"}
 :kafka-data {:bootstrap-servers "localhost:9094"}}
```

Your producer registry should expose one producer per client key:

```edn
:d-core.core.producers/registry
{:kafka-primary #ig/ref :d-core.core.producers.kafka/primary
 :kafka-data #ig/ref :d-core.core.producers.kafka/data}
```

## Dead letters

DLQ config merges in this order:

1. `[:defaults :deadletter]`
2. `[:topics <topic> :deadletter]`
3. `[:subscriptions <id> :deadletter]`

Consumers attach the subscription’s `:client` to `[:metadata :dlq :producer]`.
The producer DLQ sink uses that key to publish retries on the same cluster by
default.

## Integrant wiring example

```edn
{:system
 {:my-app.config.messaging/routing
  {:topics {:order-created {}}
   :publish {:order-created {:targets [{:producer :kafka-primary
                                        :kafka-topic "core.orders"}
                                       {:producer :kafka-data
                                        :kafka-topic "core.orders.data"}]}}
   :subscriptions {:orders-worker {:topic :order-created
                                   :source :kafka
                                   :client :kafka-primary
                                   :handler :my-app.handlers/order-created}}}

  :d-core.core.messaging/routing #ig/ref :my-app.config.messaging/routing
  ;; ... clients/producers/consumers ...
  }}
```
