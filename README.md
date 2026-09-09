# Gosch - A simple delay queue backed by redis

![build](https://github.com/cyprx/gosch/actions/workflows/go.yml/badge.svg)

Gosch is a lightweight Go library for best-effort delayed jobs. Schedule a key, then let its partition's handler load the application data and perform the work.

> **Warning:** Successful scheduling does not guarantee execution. Crashes and Redis failures can lose jobs. Use Gosch for work that tolerates loss or can be recovered independently, such as cache refreshes and periodic reconciliation. Handlers should tolerate repeated execution.

## Prerequisites

- Go 1.25+
- Redis 6.2+; local validation uses Redis 7. The queue uses [`ZRANGE BYSCORE`](https://redis.io/docs/latest/commands/zrange/) and [`RPOP count`](https://redis.io/docs/latest/commands/rpop/).

## Installation

```sh
go get github.com/cyprx/gosch
```

Import `github.com/cyprx/gosch` as `schedule`.

## Quick start

See [examples/basic.go](examples/basic.go) for Redis setup, partition registration, scheduling, and shutdown. From a checkout, with Redis running:

```sh
REDIS_URL=redis://localhost:6379/0 go run ./examples
```

The example deliberately returns handler errors to demonstrate retries. Press Ctrl+C to stop it.

## Behavior and limits

- Jobs carry keys, not application payloads. Partitions select handlers.
- Keys and partitions must be nonempty and must not contain `::`. Partitions must not contain `/`; job keys may contain `/`.
- Existing jobs with unsupported partition names are not migrated automatically.
- Scheduling the same partition/key updates its delayed entry. It does not deduplicate work already queued or running.
- Delays use whole seconds. Polling and backlog can make execution late; there is no precise timing or completion-order guarantee.
- `Remove` only removes delayed entries. It cannot cancel queued or running handlers, which may schedule another retry.
- Handler errors trigger a retry after 20 seconds. There is no acknowledgement, crash recovery, dead-letter queue, or replay API.
- Handlers receive a five-second context timeout and must honor it. This cannot forcibly stop user code. A blocked handler can block shutdown; handler panics are not recovered.

## Deployment

Configure and register partitions before calling `Run`. This is not a general thread-safe lifecycle API: do not call `Run` or `Close` concurrently with themselves. Cancel the run context, wait for `Run` to return, then call `Close` once. Cancelling `Run` alone does not stop its workers.

> **Warning:** Every replica in a namespace must register the same nonnil handlers, including during rolling deployments. All workers share one ready queue. Producers also need local partition registration to schedule jobs.

Use a separate namespace for each application or environment. Partition ownership is sticky and does not automatically rebalance when replicas are added. Extra replicas add workers but do not necessarily share existing distributor load.

| Setting | Default |
| --- | --- |
| Workers per scheduler | 5 (`WithConcurrency`) |
| Partition discovery interval | 5 seconds (`WithScanInterval`) |
| Partition lock TTL | 60 seconds (`WithLockTTL`) |
| Queue polling | 1 second, up to 5 items per poll |
| Handler timeout | 5 seconds |
| Retry delay | 20 seconds |

Each partition normally promotes at most five jobs per second. This is a polling limit, not a measured throughput guarantee. No automatic balancing, cron, workflows, priorities, or dashboard are provided.

## Known limitations pending fixes

- **Deadlines:** `Deadline` is not yet enforced consistently before execution or retry. An expired job can be retried without expiry, while a payload can expire before its first attempt. Check business expiry in the handler; do not rely on this field as a strict cutoff yet.
- **Missing handlers:** A job for an unregistered partition can currently panic the worker process. Keep handler registrations identical across replicas.
- **Options:** Nonpositive concurrency, scan intervals, or lock TTLs are not validated. Pass positive values; invalid options can stall processing, panic, or cause excessive polling.

These are correctness gaps to fix, distinct from the best-effort delivery tradeoff above.

## Testing

With Redis running:

```sh
REDIS_URL=redis://localhost:6379/0 go test ./...
```

## Contribution

Bug reports, tests, and small focused improvements are welcome.

## License

MIT
