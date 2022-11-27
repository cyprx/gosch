package schedule

import (
	"context"
	"time"

	delay "github.com/cyprx/gosch/pkg/delayqueue"
	simple "github.com/cyprx/gosch/pkg/simplequeue"
)

type Store interface {
	ListPartitions(ctx context.Context) ([]string, error)
	CreatePartition(ctx context.Context, par string) error
	AcquirePartition(ctx context.Context, par string, ttl time.Duration) (string, error)
	RenewPartition(ctx context.Context, par string, ttl time.Duration) error
	ReleasePartition(ctx context.Context, par string, token string) error
}

type DelayQueue interface {
	Push(ctx context.Context, partition string, item delay.QueueItem) error
	Remove(ctx context.Context, partition, key string) error
	Subscribe(ctx context.Context, partition string) (chan delay.QueueItem, error)
}

type SimpleQueue interface {
	Publish(ctx context.Context, it simple.QueueItem) error
	Subscribe(ctx context.Context) (chan simple.QueueItem, error)
}
