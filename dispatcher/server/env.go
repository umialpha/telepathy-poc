package server

import (
	"strings"
)

var (
	ENV_NSQ_LOOKUPD = "NSQ_LOOKUPD"
	ENV_NSQ_NSQD    = "NSQ_NSQD"
)

func EnvGetLookupds() []string {
	return strings.Split(ENV_NSQ_LOOKUPD, " ")
}
