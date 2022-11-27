package schedule

import (
	"context"
	"fmt"
	"log"
	"time"

	simple "github.com/cyprx/gosch/pkg/simplequeue"
)

// distributor consumes delay queue and distributes jobs to workers via simple queue
type distributor struct {
	dq     DelayQueue
	sq     SimpleQueue
	par    *partition
	cancel context.CancelFunc
	done   chan bool
}

func (d *distributor) run() error {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	ch, err := d.dq.Subscribe(ctx, d.par.name)
	if err != nil {
		return fmt.Errorf("distributor subscribe: %w", err)
	}
	go func() {
		ticker := time.NewTicker(45 * time.Second)
		defer func() {
			ticker.Stop()
			d.done <- true
		}()

		for {
			select {
			case <-ticker.C:
				if err := d.par.Renew(); err != nil {
					d.close()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for it := range ch {
			if err := d.sq.Publish(context.Background(), simple.QueueItem{
				Partition: d.par.name,
				Key:       it.Key,
				Timestamp: it.Score,
			}); err != nil {
				log.Printf("[ERR] failed to publish to queue: %v", err)
			}
		}
		d.done <- true
	}()
	return nil
}

func (d *distributor) close() {
	d.cancel()
	<-d.done
	<-d.done
	if err := d.par.Release(); err != nil {
		log.Printf("[ERR] failed to release partition %v: %v", d.par, err)
	}
}
