package server

import (
	"os"
	"strings"
)

var (
	ENV_NSQ_LOOKUPD    = "NSQ_LOOKUPD"
	ENV_NSQ_NSQD       = "NSQ_NSQD"
	ENV_REDIS_ADDR     = "REDIS_ADDR"
	ENV_REDIS_PASSWORD = "REDIS_PASSWORD"
)

func EnvGetLookupds() []string {
	lookupds := os.Getenv(ENV_NSQ_LOOKUPD)
	if lookupds == "" {
		lookupds = "localhost:4161"
	}
	return strings.Split(lookupds, " ")
}

func EnvGetNsqdAddr() string {
	addr := os.Getenv(ENV_NSQ_NSQD)
	if addr == "" {
		addr = "localhost:4150"
	}
	return addr
}

func EnvGetRedisAddrs() []string {
	redisAddr := os.Getenv(ENV_REDIS_ADDR)
	if redisAddr == "" {
		redisAddr = "redis-tmp.redis.cache.windows.net:6379"

	}
	return strings.Split(redisAddr, " ")
}

func EnvGetRedisPass() string {
	password := os.Getenv(ENV_REDIS_PASSWORD)
	if password == "" {
		password = "wQGnvnmyAtm49nmuOMz2nNoyHv0sdvCumVgTkE4qB6Q="
	}
	return password
}
