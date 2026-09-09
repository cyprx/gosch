package schedule

import (
	"context"
	"fmt"
	"testing"
	"time"

	delay "github.com/cyprx/gosch/pkg/delayqueue"
	"github.com/stretchr/testify/require"
)

func TestConcurrentPartitionRegistrationAndAccess(t *testing.T) {
	ctx := context.Background()
	sch := NewScheduler("concurrent", nil)
	sch.store = &registrationStore{}
	sch.delayqueue = &registrationDelayQueue{}
	handler := func(context.Context, string) error { return nil }
	require.NoError(t, sch.RegisterPartition(ctx, "orders", handler))
	item := QueueItem{Partition: "orders", Key: "order-42", Deadline: time.Now().Add(time.Hour)}
	operations := []func() error{
		func() error { return sch.RegisterPartition(ctx, "orders", handler) },
		func() error { return sch.Schedule(ctx, item) },
		func() error { return sch.Remove(ctx, "orders", "order-42") },
		func() error {
			fn := sch.getHandlerFunc("orders")
			if fn == nil {
				return fmt.Errorf("registered handler missing")
			}
			return fn(ctx, "order-42")
		},
	}
	start := make(chan struct{})
	results := make(chan error, len(operations))
	for _, operation := range operations {
		go func() {
			<-start
			for i := 0; i < 1000; i++ {
				if err := operation(); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}()
	}
	close(start)
	for range operations {
		require.NoError(t, <-results)
	}
}

type registrationStore struct {
	Store
}

func (s *registrationStore) CreatePartition(context.Context, string) error {
	return nil
}

type registrationDelayQueue struct {
	DelayQueue
}

func (q *registrationDelayQueue) Push(context.Context, string, delay.QueueItem) error {
	return nil
}

func (q *registrationDelayQueue) Remove(context.Context, string, string) error {
	return nil
}
