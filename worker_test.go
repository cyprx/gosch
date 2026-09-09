package schedule

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	simple "github.com/cyprx/gosch/pkg/simplequeue"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestWorkerRetriesAfterHandlerTimeout(t *testing.T) {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "redis://localhost:6379"
	}
	opts, err := redis.ParseURL(addr)
	require.NoError(t, err)
	rc := redis.NewClient(opts)
	defer rc.Close()
	namespace := fmt.Sprintf("worker_%d", time.Now().UnixNano())
	sch := NewScheduler(namespace, rc)
	defer func() {
		require.NoError(t, rc.Del(context.Background(),
			namespace+"/partitions", namespace+"/jobs",
			namespace+"/sorted_sets/orders", namespace+"/maps/orders/order-42",
		).Err())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handlerErr := make(chan error, 1)
	processed := make(chan struct{}, 1)
	require.NoError(t, sch.RegisterPartition(ctx, "orders", func(ctx context.Context, key string) error {
		if key == "marker" {
			processed <- struct{}{}
			return nil
		}
		<-ctx.Done()
		handlerErr <- ctx.Err()
		return ctx.Err()
	}))
	deadline := time.Now().Add(time.Hour).Unix()
	require.NoError(t, sch.simplequeue.Publish(ctx, simple.QueueItem{
		Partition: "orders", Key: "order-42", Counter: 3, Deadline: deadline,
	}))
	require.NoError(t, sch.simplequeue.Publish(ctx, simple.QueueItem{
		Partition: "orders", Key: "marker", Deadline: deadline,
	}))
	w := &worker{sch: sch, sq: sch.simplequeue, done: make(chan bool, 1)}
	require.NoError(t, w.run())
	defer w.close()

	select {
	case <-processed:
	case <-ctx.Done():
		t.Fatal("timed out waiting for worker to finish retry scheduling")
	}
	require.ErrorIs(t, <-handlerErr, context.DeadlineExceeded)
	payload, err := rc.Get(ctx, namespace+"/maps/orders/order-42").Result()
	require.NoError(t, err, "timed-out job must be rescheduled")
	require.Equal(t, fmt.Sprintf("order-42::4::%d", deadline), payload)
	score, err := rc.ZScore(ctx, namespace+"/sorted_sets/orders", namespace+"/maps/orders/order-42").Result()
	require.NoError(t, err)
	require.Greater(t, score, float64(time.Now().Unix()))
}

func TestWorkerHonorsDeadline(t *testing.T) {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "redis://localhost:6379"
	}
	opts, err := redis.ParseURL(addr)
	require.NoError(t, err)
	rc := redis.NewClient(opts)
	defer rc.Close()
	for _, scenario := range []string{"expired", "expires-during-handler", "retry-too-late"} {
		t.Run(scenario, func(t *testing.T) {
			namespace := fmt.Sprintf("worker_deadline_%d", time.Now().UnixNano())
			sch := NewScheduler(namespace, rc)
			defer func() {
				require.NoError(t, rc.Del(context.Background(), namespace+"/partitions", namespace+"/jobs",
					namespace+"/sorted_sets/orders", namespace+"/maps/orders/job").Err())
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			deadline := time.Now().Add(3 * time.Second).Unix()
			if scenario == "expired" {
				deadline = time.Now().Add(-time.Second).Unix()
			}
			processed := make(chan struct{}, 1)
			calls := make(chan time.Time, 1)
			require.NoError(t, sch.RegisterPartition(ctx, "orders", func(ctx context.Context, key string) error {
				if key == "marker" {
					processed <- struct{}{}
					return nil
				}
				handlerDeadline, _ := ctx.Deadline()
				calls <- handlerDeadline
				if scenario == "expires-during-handler" {
					time.Sleep(time.Until(time.Unix(deadline, 0)) + 10*time.Millisecond)
				}
				return errors.New("retry requested")
			}))
			require.NoError(t, sch.simplequeue.Publish(ctx, simple.QueueItem{Partition: "orders", Key: "job", Deadline: deadline}))
			require.NoError(t, sch.simplequeue.Publish(ctx, simple.QueueItem{Partition: "orders", Key: "marker", Deadline: time.Now().Add(time.Hour).Unix()}))
			w := &worker{sch: sch, sq: sch.simplequeue, done: make(chan bool, 1)}
			require.NoError(t, w.run())
			defer w.close()
			select {
			case <-processed:
			case <-ctx.Done():
				t.Fatal("worker did not finish processing")
			}
			if scenario == "expired" {
				require.Empty(t, calls, "expired jobs must not invoke handlers")
			} else {
				require.Len(t, calls, 1)
				require.Equal(t, time.Unix(deadline, 0), <-calls, "handler context must respect the job deadline")
			}
			count, err := rc.Exists(ctx, namespace+"/sorted_sets/orders", namespace+"/maps/orders/job").Result()
			require.NoError(t, err)
			require.Zero(t, count, "retry must not outlive the deadline")
		})
	}
}
