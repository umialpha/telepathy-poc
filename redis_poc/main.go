package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type message struct {
	ID         string
	Payload    []byte
	ExpireTime int64
}

func (m *message) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)

}

func (m *message) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)

}

var ctx = context.Background()

func ExampleNewClient() {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{"localhost:30001", "localhost:30002", "localhost:30003"},
		Password: "", // no password set
	})

	pipeline := rdb.Pipeline()
	// for i := 0; i < 10; i++ {
	// 	pipeline.Set(ctx, fmt.Sprintf("{session:sessionId}:%d", i), i, 10*time.Second)
	// }
	// _, err := pipeline.Exec(ctx)
	// if err != nil {
	// 	fmt.Println("loadKeyValues error", err)
	// 	return
	// }
	pipeline = rdb.Pipeline()
	for i := 0; i < 10; i++ {
		pipeline.Get(ctx, fmt.Sprintf("{session:sessionId}:%d", i))
	}
	cmds, _ := pipeline.Exec(ctx)
	for _, cmd := range cmds {
		fmt.Println(cmd.Err())
	}
}

func AzureClient() {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{"telepathy-test.redis.cache.windows.net:6379"},
		Password: "GuPi1EHSjN62iLH2FnvGWrhiPoPOThEh5hC+MFbc2a8=", // no password set
	})
	for {
		time.Sleep(2 * time.Second)
		key, err := uuid.NewUUID()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println(key)
		cmd := rdb.Set(context.Background(), key.String(), &message{ID: "123", Payload: []byte("HI"), ExpireTime: -1}, time.Second)
		fmt.Println(cmd.Result())

		fmt.Println(rdb.Get(context.Background(), key.String()).Result())

	}

}

func main() {
	ExampleNewClient()
}
