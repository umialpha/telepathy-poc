package server

import (
	"errors"
	"sync"
	"time"
)

var (
	KeyNotFoundErr = errors.New("KeyNotFoundErr")
)

type DefaultSetFunc = func() (interface{}, error)

type Cache interface {
	Set(string, interface{}, time.Duration) error
	Get(string) (interface{}, error)
	Exists(string) bool
	Delete(string) error
}

type memoryCache struct {
	sync.Mutex
	Cache
	values map[string]interface{}
}

func (mc *memoryCache) Set(key string, val interface{}, expireDuration time.Duration) error {
	mc.Lock()
	defer mc.Unlock()
	mc.values[key] = val
	return nil
}

func (mc *memoryCache) Get(key string) (interface{}, error) {
	mc.Lock()
	defer mc.Unlock()
	if val, ok := mc.values[key]; ok {
		return val, nil
	}
	return nil, KeyNotFoundErr
}

func (mc *memoryCache) Exists(key string) bool {
	val, _ := mc.Get(key)
	return val != nil
}

func (mc *memoryCache) Delete(key string) error {
	delete(mc.values, key)
	return nil
}

func NewInMemoryCache() Cache {
	return &memoryCache{values: make(map[string]interface{})}
}
