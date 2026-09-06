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

package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lonng/nano/service"
)

// NetworkEntity represent low-level network instance
type NetworkEntity interface {
	Push(route string, v interface{}) error
	RPC(route string, v interface{}) error
	LastMid() uint64
	Response(v interface{}) error
	ResponseMid(mid uint64, v interface{}) error
	Close() error
	RemoteAddr() net.Addr
}

var (
	//ErrIllegalUID represents a invalid uid
	ErrIllegalUID = errors.New("illegal uid")
)

// Session represents a client session which could storage temp data during low-level
// keep connected, all data will be released when the low-level connection was broken.
// Session instance related to the client will be passed to Handler method as the first
// parameter.
type Session struct {
	*sessionState
	requestMID   uint64
	requestBound bool
	ctx          context.Context
}

type sessionState struct {
	sync.RWMutex                        // protect data
	id           int64                  // session global unique id
	uid          int64                  // binding user id
	lastTime     int64                  // last heartbeat time
	entity       NetworkEntity          // low-level network entity
	data         map[string]interface{} // session data store
	router       *Router
	lifetime     context.Context
}

// New returns a new session instance
// a NetworkEntity is a low-level network instance
func New(entity NetworkEntity) *Session {
	return NewWithContext(context.Background(), entity)
}

// NewWithContext creates a session whose operations use the connection context.
func NewWithContext(ctx context.Context, entity NetworkEntity) *Session {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Session{ctx: ctx, sessionState: &sessionState{
		id: service.Connections.SessionID(), entity: entity,
		data: make(map[string]interface{}), lastTime: time.Now().Unix(),
		router: newRouter(), lifetime: ctx,
	}}
}

// WithRequest binds the response ID and context without copying session state
// or its mutex. Keep this view when responding from an asynchronous callback.
func (s *Session) WithRequest(ctx context.Context, mid uint64) *Session {
	if ctx == nil {
		ctx = s.Context()
	}
	return &Session{sessionState: s.sessionState, ctx: ctx, requestMID: mid, requestBound: true}
}

// Context returns the request or connection context.
func (s *Session) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// Connection returns an unbound view suitable for long-lived session storage.
func (s *Session) Connection() *Session {
	return &Session{sessionState: s.sessionState, ctx: s.sessionState.lifetime}
}

// NetworkEntity returns the low-level network agent object
func (s *Session) NetworkEntity() NetworkEntity {
	return s.entity
}

// NetworkEntity returns the service router
func (s *Session) Router() *Router {
	return s.router
}

// RPC sends message to remote server
func (s *Session) RPC(route string, v interface{}) error {
	return s.RPCContext(s.Context(), route, v)
}

// RPCContext propagates cancellation to context-aware network entities.
func (s *Session) RPCContext(ctx context.Context, route string, v interface{}) error {
	if ctx == nil {
		ctx = s.Context()
	}
	if entity, ok := s.entity.(interface {
		RPCContext(context.Context, string, interface{}) error
	}); ok {
		return entity.RPCContext(ctx, route, v)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.entity.RPC(route, v)
}

// Push message to client
func (s *Session) Push(route string, v interface{}) error {
	return s.PushContext(s.sessionState.lifetime, route, v)
}

// PushContext applies an explicit deadline to a connection push.
func (s *Session) PushContext(ctx context.Context, route string, v interface{}) error {
	if ctx == nil {
		ctx = s.sessionState.lifetime
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if entity, ok := s.entity.(interface {
		PushContext(context.Context, string, interface{}) error
	}); ok {
		return entity.PushContext(ctx, route, v)
	}
	return s.entity.Push(route, v)
}

// Response message to client
func (s *Session) Response(v interface{}) error {
	if !s.requestBound {
		if err := s.Context().Err(); err != nil {
			return err
		}
		return s.entity.Response(v)
	}
	return s.ResponseMID(s.requestMID, v)
}

// ResponseMID responses message to client, mid is
// request message ID
func (s *Session) ResponseMID(mid uint64, v interface{}) error {
	if err := s.Context().Err(); err != nil {
		return err
	}
	if entity, ok := s.entity.(interface {
		ResponseMidContext(context.Context, uint64, interface{}) error
	}); ok {
		return entity.ResponseMidContext(s.Context(), mid, v)
	}
	return s.entity.ResponseMid(mid, v)
}

// ID returns the session id
func (s *Session) ID() int64 {
	return s.id
}

// UID returns uid that bind to current session
func (s *Session) UID() int64 {
	return atomic.LoadInt64(&s.uid)
}

// LastMid returns the last message id
func (s *Session) LastMid() uint64 {
	if s.requestBound {
		return s.requestMID
	}
	return s.entity.LastMid()
}

// Bind bind UID to current session
func (s *Session) Bind(uid int64) error {
	if uid < 1 {
		return ErrIllegalUID
	}

	atomic.StoreInt64(&s.uid, uid)
	return nil
}

// Close terminate current session, session related data will not be released,
// all related data should be Clear explicitly in Session closed callback
func (s *Session) Close() {
	s.entity.Close()
}

// RemoteAddr returns the remote network address.
func (s *Session) RemoteAddr() net.Addr {
	return s.entity.RemoteAddr()
}

// Remove delete data associated with the key from session storage
func (s *Session) Remove(key string) {
	s.Lock()
	defer s.Unlock()

	delete(s.data, key)
}

// Set associates value with the key in session storage
func (s *Session) Set(key string, value interface{}) {
	s.Lock()
	defer s.Unlock()

	s.data[key] = value
}

// HasKey decides whether a key has associated value
func (s *Session) HasKey(key string) bool {
	s.RLock()
	defer s.RUnlock()

	_, has := s.data[key]
	return has
}

// Int returns the value associated with the key as a int.
func (s *Session) Int(key string) int {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int)
	if !ok {
		return 0
	}
	return value
}

// Int8 returns the value associated with the key as a int8.
func (s *Session) Int8(key string) int8 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int8)
	if !ok {
		return 0
	}
	return value
}

// Int16 returns the value associated with the key as a int16.
func (s *Session) Int16(key string) int16 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int16)
	if !ok {
		return 0
	}
	return value
}

// Int32 returns the value associated with the key as a int32.
func (s *Session) Int32(key string) int32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int32)
	if !ok {
		return 0
	}
	return value
}

// Int64 returns the value associated with the key as a int64.
func (s *Session) Int64(key string) int64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int64)
	if !ok {
		return 0
	}
	return value
}

// Uint returns the value associated with the key as a uint.
func (s *Session) Uint(key string) uint {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint)
	if !ok {
		return 0
	}
	return value
}

// Uint8 returns the value associated with the key as a uint8.
func (s *Session) Uint8(key string) uint8 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint8)
	if !ok {
		return 0
	}
	return value
}

// Uint16 returns the value associated with the key as a uint16.
func (s *Session) Uint16(key string) uint16 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint16)
	if !ok {
		return 0
	}
	return value
}

// Uint32 returns the value associated with the key as a uint32.
func (s *Session) Uint32(key string) uint32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint32)
	if !ok {
		return 0
	}
	return value
}

// Uint64 returns the value associated with the key as a uint64.
func (s *Session) Uint64(key string) uint64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint64)
	if !ok {
		return 0
	}
	return value
}

// Float32 returns the value associated with the key as a float32.
func (s *Session) Float32(key string) float32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(float32)
	if !ok {
		return 0
	}
	return value
}

// Float64 returns the value associated with the key as a float64.
func (s *Session) Float64(key string) float64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(float64)
	if !ok {
		return 0
	}
	return value
}

// String returns the value associated with the key as a string.
func (s *Session) String(key string) string {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return ""
	}

	value, ok := v.(string)
	if !ok {
		return ""
	}
	return value
}

// Value returns the value associated with the key as a interface{}.
func (s *Session) Value(key string) interface{} {
	s.RLock()
	defer s.RUnlock()

	return s.data[key]
}

// State returns a shallow snapshot. Values themselves remain caller-owned.
func (s *Session) State() map[string]interface{} {
	s.RLock()
	defer s.RUnlock()

	snapshot := make(map[string]interface{}, len(s.data))
	for key, value := range s.data {
		snapshot[key] = value
	}
	return snapshot
}

// Restore session state after reconnect
func (s *Session) Restore(data map[string]interface{}) {
	s.Lock()
	defer s.Unlock()

	s.data = make(map[string]interface{}, len(data))
	for key, value := range data {
		s.data[key] = value
	}
}

// Clear releases all data related to current session
func (s *Session) Clear() {
	s.Lock()
	defer s.Unlock()

	atomic.StoreInt64(&s.uid, 0)
	s.data = map[string]interface{}{}
}
