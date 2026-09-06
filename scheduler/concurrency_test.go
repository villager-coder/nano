package scheduler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type alwaysReady struct{}

func (alwaysReady) Check(time.Time) bool { return true }

func isolateTimers(t *testing.T) {
	t.Helper()
	timers, created, closing := timerManager.timers, timerManager.createdTimer, timerManager.closingTimer
	timerManager.timers = make(map[int64]*Timer)
	timerManager.createdTimer = nil
	timerManager.closingTimer = nil
	t.Cleanup(func() {
		timerManager.timers, timerManager.createdTimer, timerManager.closingTimer = timers, created, closing
	})
}

func TestTimerStopDuringCallback(t *testing.T) {
	isolateTimers(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls int
	timer := NewCondTimer(alwaysReady{}, func() {
		calls++
		close(entered)
		<-release
	})
	go func() {
		cron()
		close(done)
	}()
	<-entered
	timer.Stop()
	timer.Stop()
	close(release)
	<-done
	cron()
	if calls != 1 || len(timerManager.timers) != 0 {
		t.Fatalf("calls=%d, remaining timers=%d", calls, len(timerManager.timers))
	}
}

func TestTimerStopsItself(t *testing.T) {
	isolateTimers(t)
	var calls int
	var timer *Timer
	timer = NewTimer(time.Nanosecond, func() {
		calls++
		timer.Stop()
	})
	timer.createAt = 0
	cron()
	cron()
	if calls != 1 || len(timerManager.timers) != 0 {
		t.Fatalf("calls=%d, remaining timers=%d", calls, len(timerManager.timers))
	}
}

func TestTimerConcurrentCreateStop(t *testing.T) {
	isolateTimers(t)
	var wg sync.WaitGroup
	var calls atomic.Int64
	done := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				timer := NewCondTimer(alwaysReady{}, func() { calls.Add(1) })
				timer.Stop()
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	for {
		cron()
		select {
		case <-done:
			cron()
			if len(timerManager.timers) != 0 {
				t.Fatalf("stopped timers retained: %d", len(timerManager.timers))
			}
			return
		default:
		}
	}
}
