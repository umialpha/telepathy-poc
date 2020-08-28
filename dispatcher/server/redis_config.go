package server

import (
	"fmt"
	"time"
)

var (
	REDIS_ADDR_KEY       = "REDIS_ADDR"
	REDIS_PASSWORD_KEY   = "REDIS_PASSWORD"
	KEY_EXPIRED_DURATION = 24 * time.Hour
)

func SessionTaskKey(sid string, taskId string) string {
	return fmt.Sprintf("{session:%s}:%s", sid, taskId)
}

func SessionTaskResponse(sid string) string {
	return fmt.Sprintf("{session:%s}:response", sid)
}

func SessionBatchKey(sid string) string {
	return fmt.Sprintf("{session:%s:batch}", sid)
}
