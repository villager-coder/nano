package cluster

import (
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

type inlineScheduler struct{}

func (inlineScheduler) Schedule(task scheduler.Task) { task() }

type midReceiver struct{}

func (*midReceiver) Handle(*session.Session, []byte) error { return nil }

func TestConcurrentMessageID(t *testing.T) {
	for _, entity := range []session.NetworkEntity{&agent{}, &acceptor{}} {
		t.Run(reflect.TypeOf(entity).String(), func(t *testing.T) {
			s := session.New(entity)
			s.Set("inline", inlineScheduler{})
			h := &LocalHandler{localServices: map[string]*component.Service{
				"test": {SchedName: "inline"},
			}}
			receiver := &midReceiver{}
			method, _ := reflect.TypeOf(receiver).MethodByName("Handle")
			handler := &component.Handler{Receiver: reflect.ValueOf(receiver), Method: method, IsRawArg: true}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := uint64(1); i <= 10000; i++ {
					h.localProcess(handler, i, s, &message.Message{Type: message.Request, Route: "test.Handle"})
				}
			}()
			go func() {
				defer wg.Done()
				for i := 0; i < 10000; i++ {
					entity.LastMid()
				}
			}()
			wg.Wait()
			if entity.LastMid() != 10000 {
				t.Fatalf("last message ID: %d", entity.LastMid())
			}
		})
	}
}

func TestConcurrentHeartbeatRead(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	a := &agent{conn: conn}
	h := &LocalHandler{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			if err := h.processPacket(a, &packet.Packet{Type: packet.Heartbeat}); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			_ = a.String()
		}
	}()
	wg.Wait()
}

func TestAgentConcurrentSendClose(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	a := newAgent(conn, nil, nil)
	var wg sync.WaitGroup
	var closed atomic.Int32
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				if err := a.send(pendingMessage{}); err != nil && err != ErrBufferExceed && err != ErrBrokenPipe {
					t.Error(err)
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := a.Close(); err == nil {
				closed.Add(1)
			} else if err != ErrCloseClosedSession {
				t.Error(err)
			}
			a.setStatus(statusWorking)
		}()
	}
	close(start)
	wg.Wait()
	if closed.Load() != 1 || a.status() != statusClosed {
		t.Fatalf("successful closes=%d, state=%d", closed.Load(), a.status())
	}
	if err := a.send(pendingMessage{}); err != ErrBrokenPipe {
		t.Fatalf("send after close: %v", err)
	}
}

func TestAgentSendFullQueue(t *testing.T) {
	a := &agent{chDie: make(chan struct{}), chSend: make(chan pendingMessage, 1)}
	if err := a.send(pendingMessage{}); err != nil {
		t.Fatal(err)
	}
	if err := a.send(pendingMessage{}); err != ErrBufferExceed {
		t.Fatalf("full queue: %v", err)
	}
}
