package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	schedule "github.com/cyprx/gosch"
	"github.com/go-redis/redis/v8"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	redisc := redis.NewClient(&redis.Options{})
	sch := schedule.NewScheduler("test", redisc)
	partition := "par_0"
	key0 := "key_0"
	key1 := "key_1"

	if err := sch.RegisterHandlerFunc(ctx, partition, dummyHandle); err != nil {
		log.Fatal(err)
	}

	go sch.Run(ctx)

	if err := sch.Schedule(ctx, schedule.QueueItem{
		Partition:    partition,
		Key:          key0,
		DelaySeconds: 10,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	}); err != nil {
		log.Fatal(err)
	}
	if err := sch.Schedule(ctx, schedule.QueueItem{
		Partition:    partition,
		Key:          key1,
		DelaySeconds: 20,
		Deadline:     time.Now().UTC().Add(1 * time.Hour),
	}); err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
	sch.Close()
	cancel()
}

func dummyHandle(_ context.Context, key string) error {
	<-time.After(3 * time.Second)
	log.Printf("handled %s", key)
	return errors.New("something-wrong")
}
