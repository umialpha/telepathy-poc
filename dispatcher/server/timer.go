package server

import (
	 "sync"
	 "time"
	 "context"

)


type TimerItem struct {
	Tick func() bool
	Timeout func()
	TickDuration time.Duration
	ExpiredTime time.Time
	ID string
}

type Timer interface{
	Add(*TimerItem) bool
	Delete(string)
	Stop()
	
}


type SimpleTimer struct {
	Timer
	mu sync.Mutex
	items map[string]*TimerItem
	cancels map[string]context.CancelFunc
	stopCh chan int
	wg sync.WaitGroup
	
}

func (st *SimpleTimer) Add(it *TimerItem) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, ok := st.items[it.ID]; ok {
		return false
	}
	st.items[it.ID] = it
	go func(){
		st.wg.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		st.cancels[it.ID] = cancel
		st.tickItem(ctx, it)
	}()
	return true
}

func (st *SimpleTimer) Delete(key string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, ok := st.items[key]; !ok {
		return
	}
	delete(st.items, key)
	cancel := st.cancels[key]
	delete(st.cancels, key)
	cancel()
	
}

func (st *SimpleTimer) Stop() {
	close(st.stopCh)
	st.wg.Wait()

}


func (st *SimpleTimer) tickItem(ctx context.Context, it *TimerItem) {
	defer st.wg.Done()
	tick := time.Tick(it.TickDuration)
	for {
		expired := time.After(it.ExpiredTime.Sub(time.Now()))
		select {
		case <-ctx.Done():
			return
		case <-st.stopCh:
			return
		case <-tick:
			needed := it.Tick()
			if needed == false{
				st.Delete(it.ID)
				return
			}
		case <-expired:
			it.Timeout()
			st.Delete(it.ID)
			return

		}
	}
}


func NewSimpleTimer() Timer {
	t := &SimpleTimer{
		items: make(map[string]*TimerItem),
		cancels: make(map[string]context.CancelFunc),
		stopCh: make(chan int),
	}
	return t
}



type TimerMsg struct {
	TimerItem
	msg Message
	caches []Cache
	tickDuration time.Duration
}


// func (tm *TimerMsg) ID() string {
// 	return tm.msg.ID().String()
// }

// func (tm *TimerMsg) Tick() bool {
// 	for _, cache := range tm.caches {
// 		if cache.Exists(tm.msg.ID().String()) {
// 			return false
// 		}
// 	}
// 	tm.msg.Touch()
// 	return true
// }


// func (tm *TimerMsg) Timeout() {
// 	tm.msg.Requeue(-1)
// }

// func (tm *TimerMsg) TickDuration() time.Duration{
// 	return tm.tickDuration
// }

// func (tm *TimerMsg) ExpiredTime() time.Time {
// 	return tm.msg.ExpiredTime()
// }

// func NewTimerMsg(msg Message, caches []Cache, tickDuration time.Duration)  *TimerMsg{
// 	return &TimerMsg{
// 		msg: msg,
// 		caches: caches,
// 		tickDuration: tickDuration,
// 	}
// }