package server

import (
	"context"
	"sync"
	"time"
)

type TimerItem interface {
	Tick() bool
	Timeout()
	TickDuration() time.Duration
	ExpireDuration() time.Duration
	StartTime() time.Time
	SetStartTime(time.Time)
	ID() string
}

type Timer interface {
	Add(TimerItem) bool
	Delete(string)
	Stop()
	Size() int
}

type SimpleTimer struct {
	Timer
	mu      sync.Mutex
	items   map[string]TimerItem
	cancels map[string]context.CancelFunc
	stopCh  chan int
	wg      sync.WaitGroup
}

func (st *SimpleTimer) Add(it TimerItem) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, ok := st.items[it.ID()]; ok {
		return false
	}
	st.items[it.ID()] = it
	go func() {
		st.wg.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		st.cancels[it.ID()] = cancel
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

func (st *SimpleTimer) Size() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.items)
}

func (st *SimpleTimer) tickItem(ctx context.Context, it TimerItem) {
	defer st.wg.Done()
	ticker := time.NewTicker(it.TickDuration())
	defer ticker.Stop()
	expired := time.After(it.ExpireDuration())
	for {
		select {
		case <-ctx.Done():
			return
		case <-st.stopCh:
			return
		case <-ticker.C:
			needed := it.Tick()
			if needed == false {
				st.Delete(it.ID())
				return
			}
		case <-expired:
			it.Timeout()
			st.Delete(it.ID())
			return

		}
	}
}

func NewSimpleTimer() Timer {
	t := &SimpleTimer{
		items:   make(map[string]TimerItem),
		cancels: make(map[string]context.CancelFunc),
		stopCh:  make(chan int),
	}
	return t
}

type TimerMsg struct {
	TimerItem
	msg          Message
	caches       []Cache
	tickDuration time.Duration
}
