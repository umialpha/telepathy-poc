package server

import (
	"context"
	"fmt"
	"time"
	_ "time"

	"github.com/go-redis/redis/v8"
)

type redisCache struct {
	hash string
	// rdb  *redis.ClusterClient
	rdb *redis.Client
}

func execTime(t time.Time, logs string) {

	fmt.Println(logs, time.Since(t))
}

func (rc *redisCache) Set(key string, value interface{}, expireDuration time.Duration) error {
	defer execTime(time.Now(), "Redis Set Took Time: ")
	ctx := context.Background()
	pipeline := rc.rdb.Pipeline()
	pipeline.HSet(ctx, rc.hash, key, value)
	pipeline.Expire(ctx, key, expireDuration)
	_, err := pipeline.Exec(ctx)

	return err
}
func (rc *redisCache) Get(key string) (interface{}, error) {
	defer execTime(time.Now(), "Redis Get Took Time: ")
	val, err := rc.rdb.HGet(context.Background(), rc.hash, key).Result()
	return val, err
}

func (rc *redisCache) Exists(key string) bool {
	defer execTime(time.Now(), "Redis Exists Took Time: ")
	cmd := rc.rdb.HExists(context.Background(), rc.hash, key)
	r, err := cmd.Result()
	if err != nil {
		fmt.Println("Exists error", err)
		return false
	}
	return r
}

func (rc *redisCache) SetNX(key string, val interface{}, t time.Duration) error {
	defer execTime(time.Now(), "Redis SetNX Took Time: ")
	cmd := rc.rdb.HSetNX(context.Background(), rc.hash, key, val)
	val, err := cmd.Result()
	if err != nil {
		return KeyAlreadyExist
	}
	return nil

}

func (rc *redisCache) Delete(key string) error {
	defer execTime(time.Now(), "Redis Delete Took Time: ")
	cmd := rc.rdb.HDel(context.Background(), rc.hash, key)
	return cmd.Err()
}

func NewRedisCache(hashName string, opt *redis.Options /*opt *redis.ClusterOptions*/) Cache {
	return &redisCache{
		hash: hashName,
		// rdb:  redis.NewClusterClient(opt),
		rdb: redis.NewClient(opt),
	}

}
