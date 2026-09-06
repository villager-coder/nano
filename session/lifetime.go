package session

import "sync"

type (
	// LifetimeHandler represents a callback
	// that will be called when a session close or
	// session low-level connection broken.
	LifetimeHandler func(*Session)

	lifetime struct {
		// callbacks that emitted on session closed
		mu       sync.RWMutex
		onClosed []LifetimeHandler
	}
)

var Lifetime = &lifetime{}

// OnClosed set the Callback which will be called
// when session is closed Waring: session has closed.
func (lt *lifetime) OnClosed(h LifetimeHandler) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.onClosed = append(lt.onClosed, h)
}

func (lt *lifetime) Close(s *Session) {
	lt.mu.RLock()
	handlers := append([]LifetimeHandler(nil), lt.onClosed...)
	lt.mu.RUnlock()
	for _, h := range handlers {
		h(s)
	}
}
