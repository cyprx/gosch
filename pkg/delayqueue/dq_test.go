package delay

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
)

type DelayQueueTestSuite struct {
	suite.Suite

	namespace string
	redisc    *redis.Client
	queue     *Queue
}

func TestSuiteDelayQueue(t *testing.T) {
	addr := "redis://localhost:6379"
	if a := os.Getenv("REDIS_URL"); a != "" {
		addr = a
	}
	opts, err := redis.ParseURL(addr)
	if err != nil {
		panic(err)
	}
	redisc := redis.NewClient(opts)
	namespace := fmt.Sprintf("dq_%d", time.Now().Unix())
	queue := NewQueue(redisc, namespace)
	ts := &DelayQueueTestSuite{
		namespace: namespace,
		redisc:    redisc,
		queue:     queue,
	}

	suite.Run(t, ts)
}

func (s *DelayQueueTestSuite) AfterTest(_, _ string) {
	ctx := context.Background()
	iter := s.redisc.Scan(ctx, 0, "*"+s.namespace+"*", 0).Iterator()
	for iter.Next(ctx) {
		s.redisc.Del(ctx, iter.Val())
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
}

func (s *DelayQueueTestSuite) BeforeTest(_, _ string) {
}

func (s *DelayQueueTestSuite) TestPush() {
	ctx := context.Background()
	par0 := "partition_0"
	par1 := "partition_1"
	it0 := QueueItem{
		Key:     "zero",
		Score:   0,
		Counter: 1,
	}
	it1 := QueueItem{
		Key:      "-one",
		Score:    -1,
		Counter:  2,
		Deadline: 10,
	}
	it2 := QueueItem{
		Key:     "two",
		Score:   2,
		Counter: 3,
	}
	it3 := QueueItem{
		Key:     "three",
		Score:   3,
		Counter: 4,
	}

	err := s.queue.Push(ctx, par0, it0)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it1)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it2)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par1, it3)
	s.Require().NoError(err)

	zkey0 := s.queue.buildZKey(par0)
	count, err := s.redisc.ZCard(context.Background(), zkey0).Result()
	s.Require().NoError(err)
	s.Assert().Equal(int64(3), count)

	mkey0 := s.queue.buildMKey(par0, it0.Key)
	val, err := s.redisc.Get(context.Background(), mkey0).Result()
	s.Require().NoError(err)
	s.Assert().Equal("zero::1::0", val)

	mkey1 := s.queue.buildMKey(par0, it1.Key)
	val, err = s.redisc.Get(context.Background(), mkey1).Result()
	s.Require().NoError(err)
	s.Assert().Equal("-one::2::10", val)
}

func (s *DelayQueueTestSuite) TestRemove() {
	ctx := context.Background()
	par0 := "partition_0"
	par1 := "partition_1"
	it0 := QueueItem{
		Key:     "zero",
		Score:   0,
		Counter: 1,
	}
	it1 := QueueItem{
		Key:     "-one",
		Score:   -1,
		Counter: 2,
	}
	it2 := QueueItem{
		Key:     "two",
		Score:   2,
		Counter: 3,
	}
	it3 := QueueItem{
		Key:     "three",
		Score:   3,
		Counter: 4,
	}

	err := s.queue.Push(ctx, par0, it0)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it1)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it2)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par1, it3)
	s.Require().NoError(err)

	err = s.queue.Remove(ctx, par0, it0.Key)
	s.Require().NoError(err)

	zkey0 := s.queue.buildZKey(par0)
	res, err := s.redisc.ZCard(context.Background(), zkey0).Result()
	s.Require().NoError(err)
	s.Assert().Equal(int64(2), res)

	mkey0 := s.queue.buildMKey(par0, it0.Key)
	_, err = s.redisc.Get(context.Background(), mkey0).Result()
	s.Require().ErrorIs(redis.Nil, err)
}

func (s *DelayQueueTestSuite) TestSubscribe() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	par0 := "partition_0"
	par1 := "partition_1"
	it0 := QueueItem{
		Key:   "zero",
		Score: 0,
	}
	it1 := QueueItem{
		Key:   "-one",
		Score: -1,
	}
	it2 := QueueItem{
		Key:   "two",
		Score: 2,
	}
	it3 := QueueItem{
		Key:   "three",
		Score: 3,
	}

	err := s.queue.Push(ctx, par0, it0)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it1)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it2)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par1, it3)
	s.Require().NoError(err)

	ch, err := s.queue.Subscribe(ctx, par0)
	s.Require().NoError(err)

	var results []QueueItem
	for res := range ch {
		results = append(results, res)
	}

	s.Require().Equal(2, len(results))
	s.Assert().Equal(it0, results[0])
	s.Assert().Equal(it2, results[1])

	res, err := s.redisc.ZCard(context.Background(), s.queue.buildZKey(par0)).Result()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), res)

	res, err = s.redisc.ZCard(context.Background(), s.queue.buildZKey(par1)).Result()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), res)
}

func (s *DelayQueueTestSuite) TestSubscribe_WithScore() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	par0 := "partition_0"
	par1 := "partition_1"
	unix := time.Now().UTC().Unix()
	it0 := QueueItem{
		Key:   "zero",
		Score: unix + 1000,
	}
	it1 := QueueItem{
		Key:   "-one",
		Score: -1,
	}
	it2 := QueueItem{
		Key:   "two",
		Score: 2,
	}
	it3 := QueueItem{
		Key:   "three",
		Score: 3,
	}

	err := s.queue.Push(ctx, par0, it0)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it1)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par0, it2)
	s.Require().NoError(err)
	err = s.queue.Push(ctx, par1, it3)
	s.Require().NoError(err)

	ch, err := s.queue.Subscribe(ctx, par0)
	s.Require().NoError(err)

	var results []QueueItem
	for res := range ch {
		results = append(results, res)
	}

	s.Require().Equal(1, len(results))
	s.Assert().Equal(it2, results[0])
}
