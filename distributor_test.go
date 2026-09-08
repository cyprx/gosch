package schedule

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"testing/synctest"
	"time"

	delay "github.com/cyprx/gosch/pkg/delayqueue"
	simple "github.com/cyprx/gosch/pkg/simplequeue"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestDistributorPreservesRetryMetadata(t *testing.T) {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "redis://localhost:6379"
	}
	opts, err := redis.ParseURL(addr)
	require.NoError(t, err)
	rc := redis.NewClient(opts)
	defer rc.Close()
	namespace := fmt.Sprintf("distributor_%d", time.Now().UnixNano())
	sch := NewScheduler(namespace, rc)
	defer func() {
		sch.Close()
		require.NoError(t, rc.Del(context.Background(),
			namespace+"/partitions", namespace+"/jobs",
			namespace+"/sorted_sets/orders", namespace+"/maps/orders/order-42",
		).Err())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Now().Add(time.Hour).Unix()
	require.NoError(t, sch.store.CreatePartition(ctx, "orders"))
	require.NoError(t, sch.delayqueue.Push(ctx, "orders", delay.QueueItem{
		Key: "order-42", Score: 1, Counter: 3, Deadline: deadline,
	}))
	require.NoError(t, sch.distribute(ctx))
	ch, err := sch.simplequeue.Subscribe(ctx)
	require.NoError(t, err)

	select {
	case item, ok := <-ch:
		require.True(t, ok, "subscription closed before delivery")
		require.Equal(t, simple.QueueItem{
			Partition: "orders", Key: "order-42", Timestamp: 1,
			Counter: 3, Deadline: deadline,
		}, item)
	case <-ctx.Done():
		t.Fatal("timed out waiting for distributed job")
	}
}

func TestDistributorCloseAfterRenewalFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &renewalFailureStore{failed: make(chan struct{})}
		sch := &Scheduler{store: store}
		d := &distributor{
			dq:   &cancelableDelayQueue{},
			par:  &partition{sch: sch, name: "orders", token: "owner", ttl: time.Minute},
			done: make(chan bool, 2),
		}
		sch.distributors = []*distributor{d}
		require.NoError(t, d.run())
		<-store.failed
		synctest.Wait()

		closed := make(chan struct{})
		go func() {
			sch.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("scheduler shutdown blocked after lease renewal failed")
		}
	})
}

type renewalFailureStore struct {
	Store
	failed chan struct{}
}

func (s *renewalFailureStore) RenewPartition(context.Context, string, time.Duration) error {
	close(s.failed)
	return errors.New("renewal unavailable")
}

func (s *renewalFailureStore) ReleasePartition(context.Context, string, string) error {
	return nil
}

type cancelableDelayQueue struct {
	DelayQueue
}

func (q *cancelableDelayQueue) Subscribe(ctx context.Context, _ string) (chan delay.QueueItem, error) {
	ch := make(chan delay.QueueItem)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}
