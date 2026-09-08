package schedule

import (
	"context"
	"fmt"
	"os"
	"testing"
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
