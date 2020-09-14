package server

import (
	"fmt"
	"time"
)

var (
	KEY_EXPIRED_DURATION = 24 * time.Hour
)

func SessionTaskKey(sid string, taskId string) string {
	return fmt.Sprintf("{%s}:%s", sid, taskId)
}

func SessionTaskResponse(sid string, cid string) string {
	return fmt.Sprintf("{%s}:%s:response", sid, cid)
}

func SessionBatchKey(sid string) string {
	return fmt.Sprintf("{%s}:batchIds", sid)
}

func SesssionTaskSet(sid string) string {
	return fmt.Sprintf("{%s}:finTasks", sid)
}
