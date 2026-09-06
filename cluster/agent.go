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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

const (
	agentWriteBacklog = 16
)

var (
	// ErrBrokenPipe represents the low-level connection has broken.
	ErrBrokenPipe = errors.New("broken low-level pipe")
	// ErrBufferExceed indicates that the current session buffer is full and
	// can not receive more data.
	ErrBufferExceed = errors.New("session send buffer exceed")
)

type (
	// Agent corresponding a user, used for store raw conn information
	agent struct {
		closeDone chan struct{}
		// regular agent member
		session  *session.Session    // session
		conn     net.Conn            // low-level conn fd
		lastMid  uint64              // last message id
		state    int32               // current agent state
		chDie    chan struct{}       // wait for close
		chSend   chan pendingMessage // push message queue
		lastAt   int64               // last heartbeat unix time stamp
		decoder  *codec.Decoder      // binary decoder
		pipeline pipeline.Pipeline

		ctx          context.Context
		cancel       context.CancelFunc
		onClose      func()
		writeTimeout time.Duration
		rpcHandler   rpcHandler
		srv          reflect.Value // cached session reflect.Value
	}

	pendingMessage struct {
		typ     message.Type // message type
		route   string       // message route(push)
		mid     uint64       // response message id(response)
		payload interface{}  // payload
		packet  []byte       // pre-encoded control packet
	}
)

// Create new agent instance
func newAgent(conn net.Conn, pipeline pipeline.Pipeline, rpcHandler rpcHandler) *agent {
	ctx, cancel := context.WithCancel(context.Background())
	a := &agent{ctx: ctx, cancel: cancel, writeTimeout: 5 * time.Second,
		conn:       conn,
		closeDone:  make(chan struct{}),
		state:      statusStart,
		chDie:      make(chan struct{}),
		lastAt:     time.Now().Unix(),
		chSend:     make(chan pendingMessage, agentWriteBacklog),
		decoder:    codec.NewDecoder(),
		pipeline:   pipeline,
		rpcHandler: rpcHandler,
	}

	// binding session
	s := session.NewWithContext(ctx, a)
	a.session = s
	a.srv = reflect.ValueOf(s)

	return a
}

func (a *agent) send(m pendingMessage) error {
	if a.status() == statusClosed {
		return ErrBrokenPipe
	}
	select {
	case <-a.chDie:
		return ErrBrokenPipe
	case a.chSend <- m:
		return nil
	default:
		return ErrBufferExceed
	}
}

// LastMid implements the session.NetworkEntity interface
func (a *agent) LastMid() uint64 {
	return atomic.LoadUint64(&a.lastMid)
}

// Push, implementation for session.NetworkEntity interface
func (a *agent) Push(route string, v interface{}) error {
	if a.status() == statusClosed {
		return ErrBrokenPipe
	}

	if len(a.chSend) >= agentWriteBacklog {
		return ErrBufferExceed
	}

	if env.Debug {
		switch d := v.(type) {
		case []byte:
			log.Println(fmt.Sprintf("Type=Push, ID=%d, UID=%d, Route=%s, Data=%dbytes",
				a.session.ID(), a.session.UID(), route, len(d)))
		default:
			log.Println(fmt.Sprintf("Type=Push, ID=%d, UID=%d, Route=%s, Data=%+v",
				a.session.ID(), a.session.UID(), route, v))
		}
	}

	return a.send(pendingMessage{typ: message.Push, route: route, payload: v})
}

// RPC, implementation for session.NetworkEntity interface
func (a *agent) RPC(route string, v interface{}) error {
	return a.RPCContext(a.ctx, route, v)
}

func (a *agent) RPCContext(ctx context.Context, route string, v interface{}) error {
	if a.status() == statusClosed {
		return ErrBrokenPipe
	}

	// TODO: buffer
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: route,
		Data:  data,
	}
	if ctx == nil {
		ctx = a.ctx
	}
	callCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.ctx, cancel)
	defer stop()
	defer cancel()
	return a.rpcHandler(callCtx, a.session, msg, true)
}

// Response, implementation for session.NetworkEntity interface
// Response message to session
func (a *agent) Response(v interface{}) error {
	return a.ResponseMid(a.LastMid(), v)
}

// ResponseMid, implementation for session.NetworkEntity interface
// Response message to session
func (a *agent) ResponseMid(mid uint64, v interface{}) error {
	if a.status() == statusClosed {
		return ErrBrokenPipe
	}

	if mid <= 0 {
		return ErrSessionOnNotify
	}

	if len(a.chSend) >= agentWriteBacklog {
		return ErrBufferExceed
	}

	if env.Debug {
		switch d := v.(type) {
		case []byte:
			log.Println(fmt.Sprintf("Type=Response, ID=%d, UID=%d, MID=%d, Data=%dbytes",
				a.session.ID(), a.session.UID(), mid, len(d)))
		default:
			log.Println(fmt.Sprintf("Type=Response, ID=%d, UID=%d, MID=%d, Data=%+v",
				a.session.ID(), a.session.UID(), mid, v))
		}
	}

	return a.send(pendingMessage{typ: message.Response, mid: mid, payload: v})
}

// Close, implementation for session.NetworkEntity interface
// Close closes the agent, clean inner state and close low-level connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (a *agent) Close() error {
	if atomic.SwapInt32(&a.state, statusClosed) == statusClosed {
		if a.closeDone != nil {
			<-a.closeDone
		}
		return ErrCloseClosedSession
	}

	if a.closeDone != nil {
		defer close(a.closeDone)
	}
	if env.Debug {
		log.Println(fmt.Sprintf("Session closed, ID=%d, UID=%d, IP=%s",
			a.session.ID(), a.session.UID(), a.conn.RemoteAddr()))
	}

	close(a.chDie)
	if a.cancel != nil {
		a.cancel()
	}
	err := a.conn.Close()
	if a.onClose != nil {
		a.onClose()
	} else if err := scheduler.PushFinalizer(func() { session.Lifetime.Close(a.session) }); err != nil {
		log.Println(err)
	}
	return err
}

// RemoteAddr, implementation for session.NetworkEntity interface
// returns the remote network address.
func (a *agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

// String, implementation for Stringer interface
func (a *agent) String() string {
	return fmt.Sprintf("Remote=%s, LastTime=%d", a.conn.RemoteAddr().String(), atomic.LoadInt64(&a.lastAt))
}

func (a *agent) status() int32 {
	return atomic.LoadInt32(&a.state)
}

func (a *agent) setStatus(state int32) {
	for {
		old := a.status()
		if old == statusClosed || atomic.CompareAndSwapInt32(&a.state, old, state) {
			return
		}
	}
}

func (a *agent) write() {
	ticker := time.NewTicker(env.Heartbeat)
	// clean func
	defer func() {
		ticker.Stop()
		a.Close()
		if env.Debug {
			log.Println(fmt.Sprintf("Session write goroutine exit, SessionID=%d, UID=%d", a.session.ID(), a.session.UID()))
		}
	}()

	for {
		select {
		case <-ticker.C:
			deadline := time.Now().Add(-2 * env.Heartbeat).Unix()
			if atomic.LoadInt64(&a.lastAt) < deadline {
				log.Println(fmt.Sprintf("Session heartbeat timeout, LastTime=%d, Deadline=%d", atomic.LoadInt64(&a.lastAt), deadline))
				return
			}
			if a.status() == statusWorking {
				if err := a.writePacket(hbd); err != nil {
					return
				}
			}

		case data := <-a.chSend:
			if data.packet != nil {
				if err := a.writePacket(data.packet); err != nil {
					return
				}
				continue
			}
			payload, err := message.Serialize(data.payload)
			if err != nil {
				switch data.typ {
				case message.Push:
					log.Println(fmt.Sprintf("Push: %s error: %s", data.route, err.Error()))
				case message.Response:
					log.Println(fmt.Sprintf("Response message(id: %d) error: %s", data.mid, err.Error()))
				default:
					// expect
				}
				break
			}

			// construct message and encode
			m := &message.Message{
				Type:  data.typ,
				Data:  payload,
				Route: data.route,
				ID:    data.mid,
			}
			if pipe := a.pipeline; pipe != nil {
				err := pipe.Outbound().Process(a.session, m)
				if err != nil {
					log.Println("broken pipeline", err.Error())
					break
				}
			}

			em, err := m.Encode()
			if err != nil {
				log.Println(err.Error())
				break
			}

			// packet encode
			p, err := codec.Encode(packet.Data, em)
			if err != nil {
				log.Println(err)
				break
			}
			if err := a.writePacket(p); err != nil {
				return
			}

		case <-a.chDie: // agent closed signal
			return

		case <-env.Die: // application quit
			return
		}
	}
}

// writePacket is only called by the write goroutine, including for handshakes.
func (a *agent) writePacket(data []byte) error {
	timeout := a.writeTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if err := a.conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	for len(data) > 0 {
		n, err := a.conn.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
