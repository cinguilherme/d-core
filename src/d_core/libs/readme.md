## D-Core Libraries

The libs are either pure functions or macros that allow for abstractions to be wrappers of code logic.

### Retriable

Retriable is a macro, it's meant to be used as a evolution of a try catch block but with configurations to allow for:
- Retriable errors to be retried
- Maximum number of attempts to be retried
- Backoff strategy to be used

Where does it fit on D-Core Abstractions?
- Deadletters is one of the places that can benefit, if we fail a processing message due to a transient error, it may be in the interest of the system to retry in place instead of sending to a deadletter queue, not all queues are going to grow infinitely in case of slower consumer rate.

### Time

Time provides a thin ergonomic layer over `java.time` plus a Duct/Integrant
clock component that defaults to UTC. It is meant to keep time operations
consistent across the system and to make time data easy to serialize.

See `docs/time.md` for usage and configuration.
