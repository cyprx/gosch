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
			fn := w.sch.getHandlerFunc(it.Partition)
			fnctx, fncancel := context.WithTimeout(context.Background(), time.Second*5)
			if err := fn(fnctx, it.Key); err != nil {
				backoff := calcBackoff(it.Counter)
				if err := w.sch.Schedule(fnctx, QueueItem{
					Partition:    it.Partition,
					Key:          it.Key,
					DelaySeconds: backoff,
					Counter:      it.Counter + 1,
					Deadline:     time.Unix(it.Deadline, 0),
				}); err != nil {
					log.Printf("[ERR] failed to schedule: %v", err)
				}
			}
			fncancel()
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
