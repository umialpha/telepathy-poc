package server

import (
	"context"
	"time"
	_ "time"

	"github.com/go-redis/redis/v8"
)

type redisCache struct {
	hash string
	rdb  *redis.Client
}

func (rc *redisCache) Set(key string, value interface{}, expireDuration time.Duration) error {
	ctx := context.Background()
	pipeline := rc.rdb.Pipeline()
	pipeline.HSet(ctx, rc.hash, key, value)
	pipeline.Expire(ctx, key, expireDuration)
	_, err := pipeline.Exec(ctx)

	return err
}
func (rc *redisCache) Get(key string) (interface{}, error) {
	val, err := rc.rdb.HGet(context.Background(), rc.hash, key).Result()
	return val, err
}

func (rc *redisCache) Exists(key string) bool {
	cmd := rc.rdb.HExists(context.Background(), rc.hash, key)
	val, err := cmd.Result()
	if err != nil {
		return false
	}
	return val

}

func (rc *redisCache) Delete(key string) error {
	cmd := rc.rdb.HDel(context.Background(), rc.hash, key)
	return cmd.Err()
}

func NewRedisCache(hashName string, opt *redis.Options) Cache {
	return &redisCache{
		hash: hashName,
		rdb:  redis.NewClient(opt),
	}
}
