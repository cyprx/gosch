package store

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v8"
)

type Store struct {
	redisc *redis.Client
	parKey string
}

func NewStore(redisc *redis.Client, parKey string) *Store {
	return &Store{redisc: redisc, parKey: parKey}
}

func (s *Store) CreatePartition(ctx context.Context, par string) error {
	if _, err := s.redisc.HSet(ctx, s.parKey, par, 1).Result(); err != nil {
		return fmt.Errorf("hset: %w", err)
	}

	return nil
}

func (s *Store) ListPartitions(ctx context.Context) ([]string, error) {
	res, err := s.redisc.HGetAll(ctx, s.parKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}

	fmt.Println(res)
	var pars []string
	for k := range res {
		pars = append(pars, k)
	}
	return pars, nil
}

func (s *Store) AcquirePartition(ctx context.Context, par string, ttl time.Duration) (string, error) {
	token := randomString()
	res, err := s.redisc.SetNX(ctx, s.key(par), token, ttl).Result()
	if err != nil {
		return "", fmt.Errorf("set nx: %w", err)
	}
	if !res {
		return "", fmt.Errorf("acquire: failed")
	}
	return token, nil
}

func (s *Store) ReleasePartition(ctx context.Context, par string, token string) error {
	delNx := redis.NewScript(`
		if redis.call("get",KEYS[1]) == ARGV[1]
		then
			return redis.call("del",KEYS[1])
		else
			return 0
		end
	`)
	keys := []string{s.key(par)}
	_, err := delNx.Run(ctx, s.redisc, keys, token).Int()
	if err != nil {
		return fmt.Errorf("del nx: %w", err)
	}
	return nil
}

func (s *Store) RenewPartition(ctx context.Context, par string, ttl time.Duration) error {
	_, err := s.redisc.Expire(ctx, s.key(par), ttl).Result()
	if err != nil {
		return fmt.Errorf("expire: %w", err)
	}
	return nil
}

func (s *Store) key(par string) string {
	return fmt.Sprintf("%s::%s", s.parKey, par)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomString() string {
	b := make([]byte, 12)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
