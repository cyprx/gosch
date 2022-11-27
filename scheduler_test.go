package schedule_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	schedule "github.com/cyprx/gosch"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
)

type schedulerTestSuite struct {
	suite.Suite

	ch     chan string
	sch    *schedule.Scheduler
	ns     string
	redisc *redis.Client
}

func TestRunSchedulerSuite(t *testing.T) {
	addr := "redis://localhost:6379"
	if a := os.Getenv("REDIS_URL"); a != "" {
		addr = a
	}
	opts, err := redis.ParseURL(addr)
	if err != nil {
		panic(err)
	}
	redisc := redis.NewClient(opts)
	ns := fmt.Sprintf("scheduler_%d", time.Now().Unix())

	s := &schedulerTestSuite{
		redisc: redisc,
		ns:     ns,
	}

	suite.Run(t, s)
}

func (s *schedulerTestSuite) SetupTest() {
	s.sch = schedule.NewScheduler(s.ns, s.redisc, schedule.WithScanInterval(1*time.Second))
	s.ch = make(chan string)

}
func (s *schedulerTestSuite) AfterTest(_, _ string) {
	ctx := context.Background()
	iter := s.redisc.Scan(ctx, 0, "*"+s.ns+"*", 0).Iterator()
	for iter.Next(ctx) {
		s.redisc.Del(ctx, iter.Val())
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
	s.sch.Close()
}

func (s *schedulerTestSuite) BeforeTest(_, _ string) {
}

func (s *schedulerTestSuite) TestRegisterHandler_Success() {
	ctx := context.Background()
	par := "par_0"

	err := s.sch.RegisterHandlerFunc(ctx, par, s.dummyHandlerFunc1)
	s.Require().NoError(err)

	// partition should be created in redis
	i, err := s.redisc.HGet(ctx, fmt.Sprintf("%s/partitions", s.ns), "par_0").Int()
	s.Require().NoError(err)
	s.Assert().Equal(1, i)
}

func (s *schedulerTestSuite) TestSchedule_NoPartition() {
	ctx := context.Background()
	par := "par_0"

	err := s.sch.Schedule(ctx, schedule.QueueItem{
		Partition:    par,
		Key:          "some-key",
		DelaySeconds: 5,
		Counter:      3,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	})
	s.Require().ErrorIs(schedule.ErrInvalidPartition, err)
}

func (s *schedulerTestSuite) TestSchedule_Success() {
	ctx := context.Background()
	par := "par_0"

	err := s.sch.RegisterHandlerFunc(ctx, par, s.dummyHandlerFunc1)
	s.Require().NoError(err)

	err = s.sch.Schedule(ctx, schedule.QueueItem{
		Partition:    par,
		Key:          "some-key",
		DelaySeconds: 5,
		Counter:      3,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	})
	s.Require().NoError(err)
}

func (s *schedulerTestSuite) TestRemove_Success() {
	ctx := context.Background()
	par := "par_0"

	err := s.sch.RegisterHandlerFunc(ctx, par, s.dummyHandlerFunc1)
	s.Require().NoError(err)

	err = s.sch.Schedule(ctx, schedule.QueueItem{
		Partition:    par,
		Key:          "some-key",
		DelaySeconds: 5,
		Counter:      3,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	})
	s.Require().NoError(err)

	err = s.sch.Remove(ctx, par, "some-key")
	s.Require().NoError(err)
}

func (s *schedulerTestSuite) TestRun() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	par := "par_0"
	key0 := "key_0"
	key1 := "key_1"

	err := s.sch.RegisterHandlerFunc(ctx, par, s.dummyHandlerFunc2)
	s.Require().NoError(err)

	go s.sch.Run(ctx)

	err = s.sch.Schedule(ctx, schedule.QueueItem{
		Partition:    par,
		Key:          key0,
		DelaySeconds: 2,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	})
	s.Require().NoError(err)

	err = s.sch.Schedule(ctx, schedule.QueueItem{
		Partition:    par,
		Key:          key1,
		DelaySeconds: 1,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	})

	s.Require().NoError(err)

	go func() {
		<-ctx.Done()
		close(s.ch)
	}()

	var keys []string
	for key := range s.ch {
		keys = append(keys, key)
	}

	s.Require().Len(keys, 2)
	s.Assert().Equal(key1, keys[0])
	s.Assert().Equal(key0, keys[1])
}

func (s *schedulerTestSuite) TestRun_WithRemove() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	par := "par_0"
	key0 := "key_0"
	key1 := "key_1"

	err := s.sch.RegisterHandlerFunc(ctx, par, s.dummyHandlerFunc2)
	s.Require().NoError(err)

	go s.sch.Run(ctx)

	err = s.sch.Schedule(ctx, schedule.QueueItem{
		Partition:    par,
		Key:          key0,
		DelaySeconds: 2,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	})
	s.Require().NoError(err)

	err = s.sch.Schedule(ctx, schedule.QueueItem{
		Partition:    par,
		Key:          key1,
		DelaySeconds: 1,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	})
	s.Require().NoError(err)

	err = s.sch.Remove(ctx, par, key1)
	s.Require().NoError(err)

	go func() {
		<-ctx.Done()
		close(s.ch)
	}()

	var keys []string
	for key := range s.ch {
		keys = append(keys, key)
	}

	s.Require().Len(keys, 1)
	s.Assert().Equal(key0, keys[0])
}

func (s *schedulerTestSuite) dummyHandlerFunc1(ctx context.Context, key string) error {
	return nil
}

func (s *schedulerTestSuite) dummyHandlerFunc2(ctx context.Context, key string) error {
	s.ch <- key
	return nil
}
