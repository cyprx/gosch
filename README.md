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
- Handler errors trigger a retry after 20 seconds only if that retry would be due before the deadline. There is no acknowledgement, crash recovery, dead-letter queue, or replay API.
- Handlers receive a context ending at the earlier of five seconds or the job deadline and must honor it. This cannot forcibly stop user code. A blocked handler can block shutdown; handler panics are not recovered.

## Deadlines

`Deadline` is required and uses whole-second Unix precision. It must be in the future and later than the scheduled time; otherwise scheduling returns an error without storing the job. The low-level delay queue also requires a valid deadline.

Workers skip expired jobs, and retries stop when their next scheduled time would reach or exceed the deadline. Redis payloads expire at the deadline. There is no guaranteed first attempt: a job may expire while waiting for a worker. A handler already running must honor its context to stop at the deadline.

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
| Handler timeout | Up to 5 seconds, bounded by the job deadline |
| Retry delay | 20 seconds |

Each partition normally promotes at most five jobs per second. This is a polling limit, not a measured throughput guarantee. No automatic balancing, cron, workflows, priorities, or dashboard are provided.

## Known limitations pending fixes

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
