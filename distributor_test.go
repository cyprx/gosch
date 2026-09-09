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
	sch, err := NewScheduler(namespace, rc)
	require.NoError(t, err)
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

func (s *renewalFailureStore) RenewPartition(context.Context, string, string, time.Duration) error {
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

func TestPartitionRenewRequiresOwnership(t *testing.T) {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "redis://localhost:6379"
	}
	opts, err := redis.ParseURL(addr)
	require.NoError(t, err)
	rc := redis.NewClient(opts)
	defer rc.Close()
	for _, state := range []string{"owned", "missing", "replaced"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			namespace := fmt.Sprintf("renewal_%d", time.Now().UnixNano())
			sch, err := NewScheduler(namespace, rc)
			require.NoError(t, err)
			key := namespace + "/partitions::orders"
			defer rc.Del(ctx, key)
			token, err := sch.store.AcquirePartition(ctx, "orders", 10*time.Second)
			require.NoError(t, err)
			if state != "owned" {
				require.NoError(t, rc.PExpireAt(ctx, key, time.Unix(1, 0)).Err())
			}
			if state == "replaced" {
				require.NoError(t, rc.Set(ctx, key, "replacement-owner", 10*time.Second).Err())
			}
			par := &partition{sch: sch, name: "orders", token: token, ttl: time.Minute}
			err = par.Renew()
			if state == "owned" {
				require.NoError(t, err)
				ttl, err := rc.PTTL(ctx, key).Result()
				require.NoError(t, err)
				require.Greater(t, ttl, 50*time.Second)
				return
			}
			require.Error(t, err, "renewal must reject lost ownership")
			if state == "replaced" {
				value, err := rc.Get(ctx, key).Result()
				require.NoError(t, err)
				require.Equal(t, "replacement-owner", value)
				ttl, err := rc.PTTL(ctx, key).Result()
				require.NoError(t, err)
				require.LessOrEqual(t, ttl, 10*time.Second)
			}
		})
	}
}

func TestDistributorRenewsBeforeShortLeaseExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &expiringLeaseStore{expires: time.Now().Add(10 * time.Second)}
		dq := &leaseDelayQueue{stopped: make(chan struct{})}
		sch := &Scheduler{store: store}
		d := &distributor{
			dq: dq, par: &partition{sch: sch, name: "orders", token: "owner", ttl: 10 * time.Second},
			done: make(chan bool, 2),
		}
		require.NoError(t, d.run())
		defer d.close()
		time.Sleep(time.Minute)
		synctest.Wait()
		select {
		case <-dq.stopped:
			t.Fatal("distributor lost its lease because renewal was too late")
		default:
		}
	})
}

type expiringLeaseStore struct {
	Store
	expires time.Time
}

func (s *expiringLeaseStore) RenewPartition(_ context.Context, _ string, _ string, ttl time.Duration) error {
	if !time.Now().Before(s.expires) {
		return errors.New("lease expired")
	}
	s.expires = time.Now().Add(ttl)
	return nil
}

func (s *expiringLeaseStore) ReleasePartition(context.Context, string, string) error {
	return nil
}

type leaseDelayQueue struct {
	DelayQueue
	stopped chan struct{}
}

func (q *leaseDelayQueue) Subscribe(ctx context.Context, _ string) (chan delay.QueueItem, error) {
	ch := make(chan delay.QueueItem)
	go func() {
		<-ctx.Done()
		close(ch)
		close(q.stopped)
	}()
	return ch, nil
}
