// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package scheduler

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
)

type LocalScheduler interface{ Schedule(Task) }
type Task func()
type Hook func()

var (
	ErrQueueFull = errors.New("scheduler queue full")
	ErrClosed    = errors.New("scheduler closed")
	ErrNilTask   = errors.New("nil scheduler task")
	global       = newScheduler(256)
)

// scheduler owns both business tasks and connection finalizers. Finalizers have
// a separate queue so overload cannot block socket cleanup or lose close hooks.
type scheduler struct {
	mu              sync.Mutex
	tasks           chan Task
	finalizers      []Task
	wake            chan struct{}
	stop            chan struct{}
	done            chan struct{}
	started, closed bool
}

func newScheduler(capacity int) *scheduler {
	return &scheduler{tasks: make(chan Task, capacity), wake: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{})}
}

func try(f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(fmt.Sprintf("Handle message panic: %+v\n%s", err, debug.Stack()))
		}
	}()
	f()
}

func (s *scheduler) push(task Task, finalizer bool) error {
	if task == nil {
		return ErrNilTask
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if finalizer {
		s.finalizers = append(s.finalizers, task)
		select {
		case s.wake <- struct{}{}:
		default:
		}
		return nil
	}
	select {
	case s.tasks <- task:
		return nil
	default:
		return ErrQueueFull
	}
}

func (s *scheduler) finalize() {
	s.mu.Lock()
	tasks := s.finalizers
	s.finalizers = nil
	s.mu.Unlock()
	for _, task := range tasks {
		try(task)
	}
}

func (s *scheduler) drain() {
	for {
		select {
		case task := <-s.tasks:
			try(task)
		default:
			s.finalize()
			return
		}
	}
}

func (s *scheduler) run() {
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	ticker := time.NewTicker(env.TimerPrecision)
	defer ticker.Stop()
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			s.drain()
			return
		case <-ticker.C:
			cron()
		case task := <-s.tasks:
			try(task)
		case <-s.wake:
			s.finalize()
		}
	}
}

func (s *scheduler) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.done
		return
	}
	s.closed = true
	close(s.stop)
	started := s.started
	s.mu.Unlock()
	if !started {
		s.drain()
		close(s.done)
	}
	<-s.done
}

// Sched starts the process-wide logic scheduler. It may only run once.
func Sched() { global.run() }

// Close rejects new work, drains accepted tasks and finalizers, then waits for
// the scheduler to exit. Call it outside a scheduler callback.
func Close() { global.close() }

// PushTask never blocks. Callers must handle ErrQueueFull and ErrClosed.
func PushTask(task Task) error { return global.push(task, false) }

// PushFinalizer queues a connection close hook on the logic goroutine without
// consuming business queue capacity. Close drains accepted finalizers.
func PushFinalizer(task Task) error { return global.push(task, true) }
