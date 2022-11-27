package simple

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/suite"
)

type SimpleQueueTestSuite struct {
	suite.Suite

	queueName string
	redisc    *redis.Client
	queue     *Queue
}

func TestSuiteSimpleQueue(t *testing.T) {
	addr := "redis://localhost:6379"
	if a := os.Getenv("REDIS_URL"); a != "" {
		addr = a
	}
	opts, err := redis.ParseURL(addr)
	if err != nil {
		panic(err)
	}
	redisc := redis.NewClient(opts)
	queueName := fmt.Sprintf("sq_%d", time.Now().Unix())
	queue := NewQueue(redisc, queueName)
	ts := &SimpleQueueTestSuite{
		queueName: queueName,
		redisc:    redisc,
		queue:     queue,
	}

	suite.Run(t, ts)
}

func (s *SimpleQueueTestSuite) AfterTest(_, _ string) {
	ctx := context.Background()
	iter := s.redisc.Scan(ctx, 0, "*"+s.queueName+"*", 0).Iterator()
	for iter.Next(ctx) {
		s.redisc.Del(ctx, iter.Val())
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
}

func (s *SimpleQueueTestSuite) BeforeTest(_, _ string) {
}

func (s *SimpleQueueTestSuite) TestPublish() {
	ctx := context.Background()
	it0 := QueueItem{
		Partition: "partition_0",
		Key:       "key_0",
		Counter:   0,
	}
	it1 := QueueItem{
		Partition: "partition_1",
		Key:       "key_1",
		Counter:   1,
	}

	err := s.queue.Publish(ctx, it0)
	s.Require().NoError(err)
	err = s.queue.Publish(ctx, it1)
	s.Require().NoError(err)

	res, err := s.redisc.LLen(ctx, s.queueName).Result()
	s.Require().NoError(err)
	s.Assert().Equal(int64(2), res)
}

func (s *SimpleQueueTestSuite) TestSubscribe() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	it0 := QueueItem{
		Partition: "partition_0",
		Key:       "key_0",
		Counter:   0,
	}
	it1 := QueueItem{
		Partition: "partition_1",
		Key:       "key_1",
		Counter:   1,
	}

	err := s.queue.Publish(ctx, it0)
	s.Require().NoError(err)
	err = s.queue.Publish(ctx, it1)
	s.Require().NoError(err)

	ch, err := s.queue.Subscribe(ctx)
	s.Require().NoError(err)

	var results []QueueItem
	for res := range ch {
		results = append(results, res)
	}

	s.Require().Equal(2, len(results))
	s.Assert().Equal(it0, results[0])
	s.Assert().Equal(it1, results[1])
}
