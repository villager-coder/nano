package session

import (
	"sync"
	"testing"
)

func TestConcurrentBindClearUID(t *testing.T) {
	s := New(nil)
	var wg sync.WaitGroup
	for _, action := range []func(){
		func() { s.Bind(42) },
		s.Clear,
		func() {
			if uid := s.UID(); uid != 0 && uid != 42 {
				t.Errorf("invalid UID: %d", uid)
			}
		},
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				action()
			}
		}()
	}
	wg.Wait()
}
