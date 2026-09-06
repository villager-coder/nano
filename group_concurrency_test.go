package nano

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lonng/nano/session"
)

func TestGroupConcurrentClose(t *testing.T) {
	g := NewGroup("concurrent-close")
	s := session.New(nil)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var closed atomic.Int32
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				g.Add(s)
				g.Count()
				g.Members()
				g.Leave(s)
			}
			if err := g.Close(); err == nil {
				closed.Add(1)
			} else if err != ErrCloseClosedGroup {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if closed.Load() != 1 || g.Count() != 0 {
		t.Fatalf("successful closes=%d, members=%d", closed.Load(), g.Count())
	}
	if err := g.Add(s); err != ErrClosedGroup {
		t.Fatalf("add after close: %v", err)
	}
}
