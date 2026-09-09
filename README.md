# Gosch - A simple delay queue backed by redis

![build](https://github.com/cyprx/gosch/actions/workflows/go.yml/badge.svg)

Gosch is a lightweight Go library for best-effort delayed jobs. Schedule a key, then let its partition's handler load the application data and perform the work.

> **Note:** Delivery is best effort. Jobs may be lost during failures; handlers should tolerate repeated execution.

## Prerequisites

- Go 1.25+
- Redis 6.2+

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

Handle constructor errors before registering partitions or starting the scheduler:

```go
sch, err := schedule.NewScheduler("my-app", redisc)
if err != nil {
    log.Fatal(err)
}
```

## Scheduling

- Keys and partitions must be nonempty and exclude `::`. Partition names must also exclude `/`.
- Scheduling the same partition/key updates its delayed entry. `Remove` only cancels delayed entries; neither affects work already queued or running.
- Delays use whole seconds. Polling and backlog may delay execution.
- `Deadline` must be in the future and after the scheduled time. Expired jobs are skipped, even before their first attempt.
- Handler errors retry after 20 seconds, provided the retry is due before the deadline.

Handlers must honor their context, which expires after five seconds or at the job deadline, whichever comes first. Gosch cannot forcibly stop handlers or recover their panics.

## Deployment

Register partitions before `Run`. To stop, cancel its context, wait for `Run` to return, then call `Close` once. Do not call lifecycle methods concurrently. Shutdown waits for handlers to finish.

> **Note:** All replicas in a namespace must register the same handlers. Jobs received without a matching handler are logged and discarded.

Use a separate namespace per application or environment. Adding replicas adds workers; existing partition ownership does not automatically rebalance.

## Options and defaults

| Setting | Default |
| --- | --- |
| Workers per scheduler | 5 (`WithConcurrency`) |
| Partition discovery interval | 5 seconds (`WithScanInterval`) |
| Partition lock TTL | 60 seconds (`WithLockTTL`) |
| Queue polling | 1 second, up to 5 items per poll |
| Handler timeout | Up to 5 seconds, bounded by the job deadline |
| Retry delay | 20 seconds |

Concurrency and scan intervals must be positive; lock TTL must be at least 1 ms. Invalid or nil options return a constructor error.

## Testing

With Redis running:

```sh
REDIS_URL=redis://localhost:6379/0 go test ./...
```

## Contribution

Bug reports, tests, and small focused improvements are welcome.

## License

MIT
