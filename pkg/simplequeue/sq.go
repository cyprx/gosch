package simple

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	defaultItemPopped  = 5
	defaultWaitTimeout = 1 * time.Second
)

type QueueItem struct {
	Partition string
	Key       string
	Timestamp int64
	Counter   int64
	Deadline  int64
}

type Queue struct {
	name   string
	redisc *redis.Client
}

func NewQueue(rc *redis.Client, name string) *Queue {
	return &Queue{redisc: rc, name: name}
}

func (q *Queue) Publish(ctx context.Context, it QueueItem) error {
	v, err := encodeItem(it)
	if err != nil {
		return err
	}
	if cmd := q.redisc.LPush(ctx, q.name, v); cmd.Err() != nil {
		return fmt.Errorf("lpush: %w", cmd.Err())
	}
	return nil
}

func (q *Queue) Subscribe(ctx context.Context) (chan QueueItem, error) {
	ch := make(chan QueueItem)
	go func() {
		ticker := time.NewTicker(defaultWaitTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				results, err := q.redisc.RPopCount(ctx, q.name, defaultItemPopped).Result()
				if err != nil {
					continue
				}
				if len(results) == 0 {
					continue
				}
				for _, result := range results {
					var it QueueItem
					if err := decodeItem(result, &it); err != nil {
						log.Printf("[ERR] failed to decode item (%v): %v", result, err)
						continue
					}
					ch <- it
				}
			case <-ctx.Done():
				close(ch)
				return
			}
		}
	}()
	return ch, nil
}

func decodeItem(s string, it *QueueItem) error {
	ss := strings.Split(s, "::")
	if len(ss) != 5 {
		return ErrInvalidItem
	}
	it.Partition = ss[0]
	it.Key = ss[1]
	c, err := strconv.ParseInt(ss[2], 10, 64)
	if err != nil {
		return ErrInvalidItem
	}
	it.Counter = c
	ts, err := strconv.ParseInt(ss[3], 10, 64)
	if err != nil {
		return ErrInvalidItem
	}
	it.Timestamp = ts

	dl, err := strconv.ParseInt(ss[4], 10, 64)
	if err != nil {
		return ErrInvalidItem
	}
	it.Deadline = dl

	return nil
}

func encodeItem(it QueueItem) (string, error) {
	if it.Partition == "" || it.Key == "" {
		return "", ErrInvalidItem
	}
	s := fmt.Sprintf(
		"%s::%s::%d::%d::%d",
		it.Partition,
		it.Key,
		it.Counter,
		it.Timestamp,
		it.Deadline,
	)
	return s, nil
}

var ErrInvalidItem = errors.New("invalid item")
