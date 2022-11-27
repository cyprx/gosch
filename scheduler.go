package schedule

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	delay "github.com/cyprx/gosch/pkg/delayqueue"
	simple "github.com/cyprx/gosch/pkg/simplequeue"
	store "github.com/cyprx/gosch/pkg/store"

	"github.com/go-redis/redis/v8"
)

// Scheduler manages distributor and workers, it also accepts schedule request
type Scheduler struct {
	store       Store
	delayqueue  DelayQueue
	simplequeue SimpleQueue

	concurrent   int
	lockTTL      time.Duration
	scanInterval time.Duration
	partitions   map[string]HandlerFunc
	distributors []*distributor
	workers      []*worker
}

type Option interface {
	Apply(sch *Scheduler)
}

type concurrentOpt struct {
	con int
}

func (opt *concurrentOpt) Apply(sch *Scheduler) {
	sch.concurrent = opt.con
}

func WithConcurrency(c int) Option {
	return &concurrentOpt{c}
}

type lockTTLOpt struct {
	ttl time.Duration
}

func (opt *lockTTLOpt) Apply(sch *Scheduler) {
	sch.lockTTL = opt.ttl
}

func WithLockTTL(ttl time.Duration) Option {
	return &lockTTLOpt{ttl}
}

type scanIntervalLOpt struct {
	dur time.Duration
}

func (opt *scanIntervalLOpt) Apply(sch *Scheduler) {
	sch.scanInterval = opt.dur
}

func WithScanInterval(dur time.Duration) Option {
	return &scanIntervalLOpt{dur}
}

type HandlerFunc func(ctx context.Context, key string) error

type QueueItem struct {
	// Partition used to distribute queues and select handler func
	Partition string

	// Key is the unique id to identify the item
	Key string

	// DelaySeconds is number of seconds the item would be delayed until handled
	DelaySeconds int64

	// Counter is number of re-delivery
	Counter int64

	// Deadline is the timestamp when item would be discarded
	// Scheduler would try to deliver message once before discarding it
	Deadline time.Time
}

func (it QueueItem) Validate() error {
	if it.Partition == "" {
		return fmt.Errorf("empty partition")
	}
	if it.Key == "" {
		return fmt.Errorf("empty key")
	}
	if it.Counter < 0 {
		return fmt.Errorf("negative counter")
	}
	if it.Deadline.Unix() < 0 {
		return fmt.Errorf("negative deadline")
	}
	return nil
}

// NewScheduler creates an instance of scheduler, it should be safe to use across multi goroutines
func NewScheduler(namespace string, redisc *redis.Client, opts ...Option) *Scheduler {
	jobQueueName := fmt.Sprintf("%s/jobs", namespace)
	parKey := fmt.Sprintf("%s/partitions", namespace)
	rs := store.NewStore(redisc, parKey)
	sq := simple.NewQueue(redisc, jobQueueName)
	dq := delay.NewQueue(redisc, namespace)

	sch := &Scheduler{
		store:       rs,
		simplequeue: sq,
		delayqueue:  dq,

		concurrent:   5,
		scanInterval: 5 * time.Second,
		lockTTL:      60 * time.Second,
		partitions:   make(map[string]HandlerFunc),
	}
	for _, opt := range opts {
		opt.Apply(sch)
	}
	return sch
}

func (sch *Scheduler) Run(ctx context.Context) {
	go sch.work(ctx)
	ticker := time.NewTicker(sch.scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := sch.distribute(ctx); err != nil {
				log.Printf("[ERR] failed to distribute job: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (sch *Scheduler) Close() {
	for _, dis := range sch.distributors {
		dis.close()
	}
	for _, wrk := range sch.workers {
		wrk.close()
	}
}

func (sch *Scheduler) Schedule(ctx context.Context, item QueueItem) error {
	if err := item.Validate(); err != nil {
		return err
	}
	if _, ok := sch.partitions[item.Partition]; !ok {
		return ErrInvalidPartition
	}

	if err := sch.delayqueue.Push(ctx, item.Partition, delay.QueueItem{
		Key:      item.Key,
		Score:    time.Now().UTC().Add(time.Duration(item.DelaySeconds) * time.Second).Unix(),
		Counter:  item.Counter,
		Deadline: item.Deadline.Unix(),
	}); err != nil {
		return fmt.Errorf("schedule: dq push: %w", err)
	}

	return nil
}

func (sch *Scheduler) Remove(ctx context.Context, partition, key string) error {
	if _, ok := sch.partitions[partition]; !ok {
		return ErrInvalidPartition
	}
	if err := sch.delayqueue.Remove(ctx, partition, key); err != nil {
		return fmt.Errorf("schedule: dq push: %w", err)
	}

	return nil
}

func (sch *Scheduler) RegisterHandlerFunc(ctx context.Context, partition string, hdl HandlerFunc) error {
	if err := sch.store.CreatePartition(ctx, partition); err != nil {
		return fmt.Errorf("schedule: create partition: %w", err)
	}
	sch.partitions[partition] = hdl
	return nil
}

func (sch *Scheduler) work(ctx context.Context) {
	for w := 0; w < sch.concurrent; w++ {
		done := make(chan bool, 1)
		wkr := worker{id: w, sch: sch, done: done, sq: sch.simplequeue}
		sch.workers = append(sch.workers, &wkr)
		if err := wkr.run(); err != nil {
			log.Printf("[ERR] failed to run worker: %v", err)
		}
	}
	<-ctx.Done()
}

func (sch *Scheduler) distribute(ctx context.Context) error {
	// get all partitions
	partitions, err := sch.store.ListPartitions(ctx)
	if err != nil {
		return fmt.Errorf("list partitions: %w", err)
	}
	log.Printf("[DBG] partitions: %v", partitions)

	// acquire parition and start distributor
	for _, par := range partitions {
		token, err := sch.store.AcquirePartition(ctx, par, sch.lockTTL)
		if err != nil {
			continue
		}
		dis := &distributor{
			dq:   sch.delayqueue,
			sq:   sch.simplequeue,
			par:  &partition{sch: sch, name: par, token: token, ttl: sch.lockTTL},
			done: make(chan bool, 2),
		}
		sch.distributors = append(sch.distributors, dis)
		if err := dis.run(); err != nil {
			log.Printf("[ERR] failed to run distributor: %v", err)
		}
	}
	return nil

}

func (sch *Scheduler) getHandlerFunc(par string) HandlerFunc {
	return sch.partitions[par]
}

type partition struct {
	sch *Scheduler

	name  string
	token string
	ttl   time.Duration
}

func (p *partition) Renew() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := p.sch.store.RenewPartition(ctx, p.name, p.ttl); err != nil {
		return fmt.Errorf("renew partition: %w", err)
	}
	return nil
}

func (p *partition) Release() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := p.sch.store.ReleasePartition(ctx, p.name, p.token); err != nil {
		return fmt.Errorf("release partition: %w", err)
	}
	return nil
}

var ErrInvalidPartition = errors.New("invalid partition")
