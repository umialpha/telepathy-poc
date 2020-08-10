package server

import (
	"errors"
	"sync"
	"time"
)

var (
	KeyNotFoundErr  = errors.New("KeyNotFoundErr")
	KeyAlreadyExist = errors.New("KeyAlreadyExist")
)

type DefaultSetFunc = func() (interface{}, error)

type Cache interface {
	Set(string, interface{}, time.Duration) error
	Get(string) (interface{}, error)
	Delete(string) error
	SetNX(string, interface{}, time.Duration) error
	Exists(string) bool
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
	mc.Lock()
	defer mc.Unlock()
	_, ok := mc.values[key]
	return ok
}

func (mc *memoryCache) SetNX(key string, val interface{}, expireDuration time.Duration) error {
	mc.Lock()
	defer mc.Unlock()
	if _, ok := mc.values[key]; ok {
		return KeyAlreadyExist
	} else {
		mc.values[key] = val
		return nil
	}
}

func (mc *memoryCache) Delete(key string) error {
	delete(mc.values, key)
	return nil
}

func NewInMemoryCache() Cache {
	return &memoryCache{values: make(map[string]interface{})}
}
