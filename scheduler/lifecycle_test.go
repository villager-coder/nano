package scheduler

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func schedulerAwait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler stalled")
	}
}
func TestFullQueueRejectsReentrantTask(t *testing.T) {
	s := newScheduler(1)
	entered, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go s.run()
	if err := s.push(func() { close(entered); <-resume; result <- s.push(func() {}, false); close(finished) }, false); err != nil {
		t.Fatal(err)
	}
	schedulerAwait(t, entered)
	if err := s.push(func() {}, false); err != nil {
		t.Fatal(err)
	}
	close(resume)
	schedulerAwait(t, finished)
	if err := <-result; !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue: %v", err)
	}
	s.close()
}
func TestShutdownDrainsFinalizersUnderLoad(t *testing.T) {
	s := newScheduler(1)
	entered, resume := make(chan struct{}), make(chan struct{})
	var tasks, finalizers atomic.Int32
	s.push(func() { close(entered); <-resume }, false)
	go s.run()
	schedulerAwait(t, entered)
	s.push(func() { tasks.Add(1) }, false)
	if err := s.push(func() { finalizers.Add(1) }, true); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { s.close(); close(done) }()
	schedulerAwait(t, s.stop)
	if err := s.push(func() {}, false); !errors.Is(err, ErrClosed) {
		t.Fatalf("push after close: %v", err)
	}
	close(resume)
	schedulerAwait(t, done)
	if tasks.Load() != 1 || finalizers.Load() != 1 {
		t.Fatalf("tasks=%d finalizers=%d", tasks.Load(), finalizers.Load())
	}
	s.close()
}
func TestCloseBeforeStart(t *testing.T) {
	s := newScheduler(1)
	var called bool
	s.push(func() { called = true }, true)
	s.close()
	s.run()
	if !called {
		t.Fatal("finalizer lost")
	}
	if err := s.push(func() {}, true); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
