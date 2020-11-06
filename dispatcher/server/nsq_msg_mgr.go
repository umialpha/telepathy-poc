package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	BATCH_CHANNEL_SIZE = 100000
	BATCH_SIZE         = 100000
	BATCH_WORKERS      = 5
	MSG_STATE_SUCCESS  = "success"
	MSG_STATE_REQUEUE  = "requeue"
)

type nsqTimerItem struct {
	TimerItem
	msg          Message
	startTime    time.Time
	tickDuration time.Duration
	ch           chan Message
	mgr          *msgMgr
}

func (n *nsqTimerItem) Tick() bool {
	n.ch <- n.msg
	return true
}

func (n *nsqTimerItem) Timeout() {
	fmt.Println("Message Timeout")
	n.msg.Requeue(-1)
	n.mgr.Delete(n.ID())
}

func (n *nsqTimerItem) TickDuration() time.Duration {
	return n.tickDuration
}

func (n *nsqTimerItem) ExpireDuration() time.Duration {
	return NsqMessageTimeout
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
	mtx       sync.RWMutex
	sessionId string
	msgs      map[string]Message
	states    map[string]string
	rdb       *redis.ClusterClient
	timer     Timer
	batchCh   chan Message
	stopCh    chan int
}

func (t *msgMgr) Add(m Message) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	id := m.GetID().String()
	// if _, ok := t.msgs[id]; ok {
	// 	fmt.Println("Message Already in msgMgr", id)
	// 	return
	// }
	item := &nsqTimerItem{
		msg:          m,
		tickDuration: 1 * time.Second,
		ch:           t.batchCh,
		mgr:          t,
	}
	t.msgs[id] = m
	t.timer.Add(item)
}

func (t *msgMgr) Delete(id string) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	delete(t.msgs, id)
	delete(t.states, id)
	t.timer.Delete(id)
}

func (t *msgMgr) GetState(m Message) (string, bool) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	result, ok := t.states[m.GetID().String()]
	return result, ok

}

func (t *msgMgr) loadKeyValues(msgs map[string]Message) {
	t.mtx.Lock()
	fmt.Println("loadKeyValues", len(msgs), t.timer.Size())
	t.mtx.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pipeline := t.rdb.Pipeline()
	cmds := map[string]*redis.StringCmd{}
	for _, m := range msgs {
		id := m.GetID().String()
		cmds[id] = pipeline.Get(ctx, SessionTaskKey(t.sessionId, id))
	}
	pipeline.Exec(ctx)
	t.mtx.Lock()
	var requeueList []string
	for k, v := range cmds {
		val, err := v.Result()
		if _, ok := t.msgs[k]; !ok {
			continue
		}
		if err != nil {
			t.msgs[k].Touch()
			continue
		}
		t.timer.Delete(k)
		if val == MSG_STATE_SUCCESS {
			t.msgs[k].Finish()
			t.states[k] = MSG_STATE_SUCCESS

		} else if val == "requeue" {
			fmt.Println("Requeue", k)
			t.msgs[k].Requeue(0)
			requeueList = append(requeueList, k)
			delete(t.msgs, k)

		}
	}
	t.mtx.Unlock()
	if len(requeueList) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pipeline := t.rdb.Pipeline()
		for _, key := range requeueList {
			pipeline.Del(ctx, SessionTaskKey(t.sessionId, key))
		}
		pipeline.Exec(ctx)
	}

}

func (t *msgMgr) batch() {
	for {
		msgs := make(map[string]Message)
		batchDuration := 1 * time.Second
		timeout := time.After(batchDuration)
		for len(msgs) < BATCH_SIZE {
			select {
			case <-timeout:
				goto OuterLoop
			case msg := <-t.batchCh:
				msgs[msg.GetID().String()] = msg
				break
			case <-t.stopCh:
				return

			}
		}
	OuterLoop:
		if len(msgs) != 0 {
			t.loadKeyValues(msgs)
		}
	}
}

func (m *msgMgr) initRedisClient() {

	opt := &redis.ClusterOptions{
		Addrs:    EnvGetRedisAddrs(),
		Password: EnvGetRedisPass(), // no password set
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
	for i := 0; i < BATCH_WORKERS; i++ {
		go m.batch()
	}
	return m
}
