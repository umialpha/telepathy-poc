package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	BATCH_CHANNEL_SIZE = 1000
	BATCH_SIZE         = 100
)

type nsqTimerItem struct {
	TimerItem
	msg          Message
	startTime    time.Time
	tickDuration time.Duration
	ch           chan Message
}

func (n *nsqTimerItem) Tick() bool {
	n.ch <- n.msg
	return true
}

func (n *nsqTimerItem) Timeout() {
	n.msg.Requeue(-1)
}

func (n *nsqTimerItem) TickDuration() time.Duration {
	return n.tickDuration
}

func (n *nsqTimerItem) ExpireDuration() time.Duration {
	return n.msg.ExpiredTime().Sub(n.StartTime())
}

func (n *nsqTimerItem) StartTime() time.Time {
	return n.startTime
}

func (n *nsqTimerItem) SetStartTime(start time.Time) {
	n.startTime = start
}

func (n *nsqTimerItem) ID() string {
	return n.msg.GetID().String()
}

type msgMgr struct {
	sync.RWMutex
	sessionId string
	msgs      map[string]Message
	states    map[string]string
	rdb       *redis.ClusterClient
	timer     Timer
	batchCh   chan Message
	stopCh    chan int
}

func (t *msgMgr) Add(m Message) {
	t.RLock()
	defer t.RUnlock()
	id := m.GetID().String()
	if _, ok := t.msgs[id]; ok {
		fmt.Println("Message Already in msgMgr", id)
		return
	}
	item := &nsqTimerItem{
		msg:          m,
		tickDuration: 1 * time.Second,
		ch:           t.batchCh,
	}
	t.msgs[id] = m
	t.timer.Add(item)
}

func (t *msgMgr) GetState(m Message) (string, bool) {
	t.RLock()
	defer t.RUnlock()
	result, ok := t.states[m.GetID().String()]
	return result, ok

}

func (t *msgMgr) loadKeyValues(lst []Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pipeline := t.rdb.Pipeline()
	cmds := map[string]*redis.StringCmd{}
	for _, m := range lst {
		id := m.GetID().String()
		cmds[id] = pipeline.Get(ctx, SessionTaskKey(t.sessionId, id))
	}
	_, err := pipeline.Exec(ctx)
	if err != nil {
		fmt.Println("loadKeyValues error", err)
		return
	}
	t.Lock()
	var requeueList []string
	for k, v := range cmds {
		val, err := v.Result()
		if err == nil {
			continue
		}
		if val == "success" {
			t.msgs[k].Finish()
		} else if val == "fail" {
			t.msgs[k].Requeue(-1)
			requeueList = append(requeueList, k)
		}
	}
	t.Unlock()
	if len(requeueList) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pipeline := t.rdb.Pipeline()
		for _, key := range requeueList {
			pipeline.Del(ctx, SessionTaskKey(t.sessionId, key))
		}
	}
	_, err = pipeline.Exec(ctx)
	if err != nil {
		fmt.Println("loadKeyValues error", err)
		return
	}

}

func (t *msgMgr) batch() {
	for {
		var lst []Message
		batchDuration := 1 * time.Second
		timeout := time.After(batchDuration)
		for {
			select {
			case <-timeout:
				break
			case msg := <-t.batchCh:
				lst = append(lst, msg)
				if len(lst) >= BATCH_SIZE {
					break
				}
			case <-t.stopCh:
				return

			}
		}
		if len(lst) != 0 {
			t.loadKeyValues(lst)
		}
	}
}

func (m *msgMgr) initRedisClient() {
	redisAddr := os.Getenv(REDIS_ADDR_KEY)
	if redisAddr == "" {
		redisAddr = "localhost:6379"

	}
	redisPass := os.Getenv(REDIS_PASSWORD_KEY)
	fmt.Println("redisAddr", redisAddr, redisPass)

	opt := &redis.ClusterOptions{
		Addrs:    []string{redisAddr},
		Password: redisPass, // no password set
	}
	m.rdb = redis.NewClusterClient(opt)
}

func newMgr(sessionId string) *msgMgr {
	m := &msgMgr{
		sessionId: sessionId,
		msgs:      make(map[string]Message),
		states:    make(map[string]string),
		timer:     NewTimingWheel(),
		batchCh:   make(chan Message, BATCH_CHANNEL_SIZE),
		stopCh:    make(chan int),
	}
	m.initRedisClient()

	return m
}
