package delay

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
	Counter  int64
	Key      string
	Deadline int64
	Score    int64
}

type Queue struct {
	namespace string
	redisc    *redis.Client
}

func NewQueue(rc *redis.Client, namespace string) *Queue {
	return &Queue{redisc: rc, namespace: namespace}
}

func (q *Queue) Push(ctx context.Context, partition string, it QueueItem) error {
	if partition == "" {
		return ErrInvalidInput
	}
	zkey := q.buildZKey(partition)
	mkey := q.buildMKey(partition, it.Key)
	val, err := encodeItemValue(it)
	if err != nil {
		return err
	}

	// there is a potential race condition when a pipeline is being executed and
	// a Remove command is sent, however, since no bad effect expected, we ignore that case
	pipe := q.redisc.Pipeline()
	pipe.ZAdd(ctx, zkey, &redis.Z{Score: float64(it.Score), Member: mkey})
	pipe.Set(ctx, mkey, val, time.Duration(it.Deadline-time.Now().Unix())*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func (q *Queue) Remove(ctx context.Context, partition string, key string) error {
	if partition == "" {
		return ErrInvalidInput
	}
	zkey := q.buildZKey(partition)
	mkey := q.buildMKey(partition, key)
	pipe := q.redisc.Pipeline()
	pipe.ZRem(ctx, zkey, mkey)
	pipe.Del(ctx, mkey)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func (q *Queue) Subscribe(ctx context.Context, partition string) (chan QueueItem, error) {
	ch := make(chan QueueItem)
	zkey := q.buildZKey(partition)
	go func() {
		ticker := time.NewTicker(defaultWaitTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				zRangeRem := redis.NewScript(`
					local values = redis.call("zrange",KEYS[1], 0, ARGV[1], "BYSCORE", "LIMIT", 0, ARGV[2], "WITHSCORES")
					local results = {}
					for i, mem in ipairs(values) do 
					    if (i-1) % 2 == 0 then
							redis.call("zrem", KEYS[1], mem)
							local result = redis.call("get", mem)
							results[i] = result
						else
							results[i] = mem
						end
					end

					return results
				`)
				results, err := zRangeRem.Run(ctx, q.redisc, []string{zkey}, time.Now().UTC().Unix(), defaultItemPopped).StringSlice()
				if err != nil {
					log.Printf("[ERR] failed to pop item(%v): %v", zkey, err)
				}
				for i := 0; i < len(results); i += 2 {
					ts, err := strconv.ParseInt(results[i+1], 10, 64)
					if err != nil {
						log.Printf("[ERR] failed to parse item(%v): %v", results, err)
						break
					}
					it := QueueItem{
						Score: ts,
					}
					if err := decodeItemValue(results[i], &it); err != nil {
						log.Printf("[ERR] failed to parse item(%v): %v", results, err)
						break
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

func (q *Queue) buildZKey(par string) string {
	return fmt.Sprintf("%s/sorted_sets/%s", q.namespace, par)
}

func (q *Queue) buildMKey(par string, val string) string {
	return fmt.Sprintf("%s/maps/%s/%s", q.namespace, par, val)
}

func decodeItemValue(s string, it *QueueItem) error {
	ss := strings.Split(s, "::")
	if len(ss) != 3 {
		return ErrInvalidItem
	}
	it.Key = ss[0]

	c, err := strconv.ParseInt(ss[1], 10, 64)
	if err != nil {
		return ErrInvalidItem
	}
	it.Counter = c

	d, err := strconv.ParseInt(ss[2], 10, 64)
	if err != nil {
		return ErrInvalidItem
	}
	it.Deadline = d

	return nil
}

func encodeItemValue(it QueueItem) (string, error) {
	s := fmt.Sprintf("%s::%d::%d", it.Key, it.Counter, it.Deadline)
	return s, nil
}

var (
	ErrInvalidItem  = errors.New("invalid item")
	ErrInvalidInput = errors.New("invalid input")
)
