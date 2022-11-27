package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	suite.Suite

	parKey string
	redisc *redis.Client
	store  *Store
}

func TestSuiteStore(t *testing.T) {
	addr := "redis://localhost:6379"
	if a := os.Getenv("REDIS_URL"); a != "" {
		addr = a
	}
	opts, err := redis.ParseURL(addr)
	if err != nil {
		panic(err)
	}
	redisc := redis.NewClient(opts)
	parKey := fmt.Sprintf("store_%d", time.Now().Unix())
	store := NewStore(redisc, parKey)
	ts := &StoreTestSuite{
		redisc: redisc,
		store:  store,
		parKey: parKey,
	}

	suite.Run(t, ts)
}

func (s *StoreTestSuite) AfterTest(_, _ string) {
	ctx := context.Background()
	iter := s.redisc.Scan(ctx, 0, "*"+s.parKey+"*", 0).Iterator()
	for iter.Next(ctx) {
		s.redisc.Del(ctx, iter.Val())
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
}

func (s *StoreTestSuite) BeforeTest(_, _ string) {
	ctx := context.Background()

	s.redisc.HMSet(ctx, s.parKey, "par_0", 0, "par_1", 1)
}

func (s *StoreTestSuite) TestListPartitions() {
	ctx := context.Background()
	partitions, err := s.store.ListPartitions(ctx)
	s.Require().NoError(err)
	s.Assert().Len(partitions, 2)
	s.Assert().Contains(partitions, "par_0")
}

func (s *StoreTestSuite) TestAcquirePartition() {
	ctx := context.Background()

	token, err := s.store.AcquirePartition(ctx, "par_0", 30*time.Second)
	s.Assert().NoError(err)
	s.Assert().NotEmpty(token)

	_, err = s.store.AcquirePartition(ctx, "par_0", 30*time.Second)
	s.Assert().Error(err)

	token, err = s.store.AcquirePartition(ctx, "par_1", 30*time.Second)
	s.Assert().NoError(err)
	s.Assert().NotEmpty(token)
}

func (s *StoreTestSuite) TestReleasePartition() {
	ctx := context.Background()
	token0, err := s.store.AcquirePartition(ctx, "par_0", 30*time.Second)
	s.Assert().NoError(err)
	s.Assert().NotEmpty(token0)

	token1, err := s.store.AcquirePartition(ctx, "par_1", 30*time.Second)
	s.Assert().NoError(err)

	err = s.store.ReleasePartition(ctx, "par_0", token0)
	s.Assert().NoError(err)

	err = s.store.ReleasePartition(ctx, "par_1", "wrong-token")
	s.Assert().NoError(err)

	_, err = s.redisc.Get(ctx, s.store.key("par_0")).Result()
	s.Assert().ErrorIs(redis.Nil, err)

	res, err := s.redisc.Get(ctx, s.store.key("par_1")).Result()
	s.Assert().NoError(err)
	s.Assert().Equal(token1, res)
}
