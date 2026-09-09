package schedule

import (
	"context"
	"fmt"
	"log"
	"time"
)

// workers receive scheduled job and execute dedicated handlerFunc
type worker struct {
	id     int
	sq     SimpleQueue
	done   chan bool
	cancel context.CancelFunc

	sch *Scheduler
}

func (w *worker) run() error {
	subctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	ch, err := w.sq.Subscribe(subctx)
	if err != nil {
		return fmt.Errorf("worker subscribe: %w", err)
	}
	go func() {
		for it := range ch {
			deadline := time.Unix(it.Deadline, 0)
			if !time.Now().Before(deadline) {
				continue
			}
			fn := w.sch.getHandlerFunc(it.Partition)
			if fn == nil {
				log.Printf("[ERR] missing handler for partition %q; discarding job %q", it.Partition, it.Key)
				continue
			}
			handlerDeadline := time.Now().Add(5 * time.Second)
			if deadline.Before(handlerDeadline) {
				handlerDeadline = deadline
			}
			fnctx, fncancel := context.WithDeadline(context.Background(), handlerDeadline)
			err := fn(fnctx, it.Key)
			fncancel()
			if err != nil {
				backoff := calcBackoff(it.Counter)
				if !time.Now().Add(time.Duration(backoff) * time.Second).Before(deadline) {
					continue
				}
				retryctx, retrycancel := context.WithTimeout(context.Background(), time.Second*5)
				if err := w.sch.Schedule(retryctx, QueueItem{
					Partition:    it.Partition,
					Key:          it.Key,
					DelaySeconds: backoff,
					Counter:      it.Counter + 1,
					Deadline:     time.Unix(it.Deadline, 0),
				}); err != nil {
					log.Printf("[ERR] failed to schedule: %v", err)
				}
				retrycancel()
			}
		}
		w.done <- true
	}()
	return nil
}

func (w *worker) close() {
	w.cancel()
	<-w.done
}

func calcBackoff(counter int64) int64 {
	return 20
}
