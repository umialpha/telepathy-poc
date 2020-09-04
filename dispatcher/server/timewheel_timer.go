package server

import (
	"sync"
	"time"

	"github.com/RussellLuo/timingwheel"
)

type twTimer struct {
	Timer
	tw     *timingwheel.TimingWheel
	mu     sync.Mutex
	items  map[string]TimerItem
	timers map[string]*timingwheel.Timer
}

func (t *twTimer) tickItem(it TimerItem) {
	needed := it.Tick()
	if time.Now().After(it.StartTime().Add(it.ExpireDuration())) {
		it.Timeout()
		t.Delete(it.ID())
		return
	}

	if needed {
		t.mu.Lock()
		t.timers[it.ID()] = t.tw.AfterFunc(it.TickDuration(), func() {
			t.tickItem(it)
		})
		t.mu.Unlock()

	} else {
		t.Delete(it.ID())
	}
}

func (t *twTimer) Add(it TimerItem) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.items[it.ID()]; ok {
		return false
	}
	t.items[it.ID()] = it
	it.SetStartTime(time.Now())
	timer := t.tw.AfterFunc(it.TickDuration(), func() { t.tickItem(it) })
	t.timers[it.ID()] = timer
	return true
}

func (t *twTimer) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.items[key]; !ok {
		return
	}

	timer := t.timers[key]
	timer.Stop()
	delete(t.items, key)
	delete(t.timers, key)

}

func (t *twTimer) Stop() {
	t.tw.Stop()
}

func (st *twTimer) Size() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.timers)
}

func NewTimingWheel() Timer {
	t := &twTimer{
		items:  make(map[string]TimerItem),
		timers: make(map[string]*timingwheel.Timer),
		tw:     timingwheel.NewTimingWheel(100*time.Millisecond, 100),
	}
	t.tw.Start()
	return t
}
