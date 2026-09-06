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
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

// The heartbeat packet is immutable; handshakes are cached per handler.
var hbd = []byte{byte(packet.Heartbeat), 0, 0, 0}

type rpcHandler func(context.Context, *session.Session, *message.Message, bool) error

// CustomerRemoteServiceRoute customer remote service route
type CustomerRemoteServiceRoute func(service string, session *session.Session, members []*clusterpb.MemberInfo) *clusterpb.MemberInfo

func handshakeResponse() []byte {
	hrdata := map[string]interface{}{
		"code": 200,
		"sys": map[string]interface{}{
			"heartbeat":  env.Heartbeat.Seconds(),
			"servertime": time.Now().UTC().Unix(),
		},
	}
	if dict, ok := message.GetDictionary(); ok {
		hrdata = map[string]interface{}{
			"code": 200,
			"sys": map[string]interface{}{
				"heartbeat":  env.Heartbeat.Seconds(),
				"servertime": time.Now().UTC().Unix(),
				"dict":       dict,
			},
		}
	}
	// data, err := json.Marshal(map[string]interface{}{
	// 	"code": 200,
	// 	"sys": map[string]float64{
	// 		"heartbeat": env.Heartbeat.Seconds(),
	// 	},
	// })
	data, err := json.Marshal(hrdata)
	if err != nil {
		panic(err)
	}

	hrd, err := codec.Encode(packet.Handshake, data)
	if err != nil {
		panic(err)
	}

	return hrd
}

type LocalHandler struct {
	handshake     []byte
	localServices map[string]*component.Service // all registered service
	localHandlers map[string]*component.Handler // all handler method

	mu             sync.RWMutex
	remoteServices map[string][]*clusterpb.MemberInfo

	pipeline    pipeline.Pipeline
	currentNode *Node
}

func NewHandler(currentNode *Node, pipeline pipeline.Pipeline) *LocalHandler {
	h := &LocalHandler{
		handshake:      handshakeResponse(),
		localServices:  make(map[string]*component.Service),
		localHandlers:  make(map[string]*component.Handler),
		remoteServices: map[string][]*clusterpb.MemberInfo{},
		pipeline:       pipeline,
		currentNode:    currentNode,
	}

	return h
}

func (h *LocalHandler) register(comp component.Component, opts []component.Option) error {
	s := component.NewService(comp, opts)

	if _, ok := h.localServices[s.Name]; ok {
		return fmt.Errorf("handler: service already defined: %s", s.Name)
	}

	if err := s.ExtractHandler(); err != nil {
		return err
	}

	// register all localHandlers
	h.localServices[s.Name] = s
	for name, handler := range s.Handlers {
		n := fmt.Sprintf("%s.%s", s.Name, name)
		log.Println("Register local handler", n)
		h.localHandlers[n] = handler
	}
	return nil
}

func (h *LocalHandler) initRemoteService(members []*clusterpb.MemberInfo) {
	for _, m := range members {
		h.addRemoteService(m)
	}
}

func (h *LocalHandler) addRemoteService(member *clusterpb.MemberInfo) {
	if member == nil || member.ServiceAddr == "" {
		return
	}
	member = cloneMemberInfo(member)
	h.mu.Lock()
	defer h.mu.Unlock()
	// A registration replaces the node's complete service list.
	h.deleteRemoteLocked(member.ServiceAddr)
	seen := make(map[string]bool)
	for _, name := range member.Services {
		if !seen[name] {
			h.remoteServices[name] = append(h.remoteServices[name], member)
			seen[name] = true
		}
	}
}

func (h *LocalHandler) deleteRemoteLocked(addr string) {
	for name, members := range h.remoteServices {
		kept := make([]*clusterpb.MemberInfo, 0, len(members))
		for _, member := range members {
			if member.ServiceAddr != addr {
				kept = append(kept, member)
			}
		}
		if len(kept) == 0 {
			delete(h.remoteServices, name)
		} else {
			h.remoteServices[name] = kept
		}
	}
}

func (h *LocalHandler) delMember(addr string) {
	h.mu.Lock()
	h.deleteRemoteLocked(addr)
	h.mu.Unlock()
}

func (h *LocalHandler) LocalService() []string {
	var result []string
	for service := range h.localServices {
		result = append(result, service)
	}
	sort.Strings(result)
	return result
}

func (h *LocalHandler) RemoteService() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result []string
	for service := range h.remoteServices {
		result = append(result, service)
	}
	sort.Strings(result)
	return result
}

func (h *LocalHandler) handle(conn net.Conn) {
	// create a client agent and startup write gorontine
	agent := newAgent(conn, h.pipeline, h.remoteProcess)
	agent.writeTimeout = h.currentNode.writeTimeout()
	agent.onClose = func() {
		h.currentNode.removeSession(agent.session.ID(), agent.session)
		h.currentNode.finalizeSession(agent.session)
	}
	if !h.currentNode.storeSession(agent.session) {
		agent.Close()
		return
	}

	// startup write goroutine
	writeDone := make(chan struct{})
	go func() { defer close(writeDone); agent.write() }()

	if env.Debug {
		log.Println(fmt.Sprintf("New session established: %s", agent.String()))
	}

	defer func() {
		// Close the socket and remove local state before contacting any peer.
		agent.Close()
		<-writeDone
		if h.currentNode.rpcClient == nil {
			return
		}
		ctx, cancel := h.currentNode.rpcContext(context.Background())
		defer cancel()
		request := &clusterpb.SessionClosedRequest{SessionId: agent.session.ID()}
		for _, remote := range h.currentNode.cluster.remoteAddrs() {
			if remote == h.currentNode.ServiceAddr {
				continue
			}
			pool, err := h.currentNode.rpcClient.getConnPoolContext(ctx, remote)
			if err != nil {
				log.Println(err)
				continue
			}
			_, err = clusterpb.NewMemberClient(pool.Get()).SessionClosed(ctx, request)
			if err != nil {
				log.Println("Notify session close failed", remote, err)
			}
		}
	}()

	// read loop
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Println(fmt.Sprintf("Read message error: %s, session will be closed immediately", err.Error()))
			return
		}

		// TODO(warning): decoder use slice for performance, packet data should be copy before next Decode
		packets, err := agent.decoder.Decode(buf[:n])
		if err != nil {
			log.Println(err.Error())

			// process packets decoded
			for _, p := range packets {
				if err := h.processPacket(agent, p); err != nil {
					log.Println(err.Error())
					return
				}
			}
			return
		}

		// process all packets
		for _, p := range packets {
			if err := h.processPacket(agent, p); err != nil {
				log.Println(err.Error())
				return
			}
		}
	}
}

func (h *LocalHandler) processPacket(agent *agent, p *packet.Packet) error {
	switch p.Type {
	case packet.Handshake:
		if agent.status() != statusStart {
			return fmt.Errorf("unexpected handshake")
		}
		if err := env.HandshakeValidator(agent.session, p.Data); err != nil {
			return err
		}

		if err := agent.send(pendingMessage{packet: h.handshake}); err != nil {
			return err
		}

		if !atomic.CompareAndSwapInt32(&agent.state, statusStart, statusHandshake) {
			return ErrBrokenPipe
		}
		if env.Debug {
			log.Println(fmt.Sprintf("Session handshake Id=%d, Remote=%s", agent.session.ID(), agent.conn.RemoteAddr()))
		}

	case packet.HandshakeAck:
		if !atomic.CompareAndSwapInt32(&agent.state, statusHandshake, statusWorking) {
			return fmt.Errorf("unexpected handshake ACK")
		}
		if env.Debug {
			log.Println(fmt.Sprintf("Receive handshake ACK Id=%d, Remote=%s", agent.session.ID(), agent.conn.RemoteAddr()))
		}

	case packet.Data:
		if agent.status() != statusWorking {
			return fmt.Errorf("receive data on socket which not yet ACK, session will be closed immediately, remote=%s",
				agent.conn.RemoteAddr().String())
		}

		msg, err := message.Decode(p.Data)
		if err != nil {
			return err
		}
		if err := h.processMessage(agent, msg); err != nil {
			return err
		}

	case packet.Heartbeat:
		// Heartbeats do not advance the handshake state.
	default:
		return fmt.Errorf("unexpected client packet type: %d", p.Type)
	}

	atomic.StoreInt64(&agent.lastAt, time.Now().Unix())
	return nil
}

func (h *LocalHandler) findMembers(service string) []*clusterpb.MemberInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	members := h.remoteServices[service]
	result := make([]*clusterpb.MemberInfo, 0, len(members))
	for _, member := range members {
		result = append(result, cloneMemberInfo(member))
	}
	return result
}

func (h *LocalHandler) remoteProcess(ctx context.Context, s *session.Session, msg *message.Message, noCopy bool) error {
	index := strings.LastIndex(msg.Route, ".")
	if index <= 0 {
		return fmt.Errorf("invalid route: %s", msg.Route)
	}
	service := msg.Route[:index]
	members := h.findMembers(service)
	if len(members) == 0 {
		s.Router().Delete(service)
		return fmt.Errorf("remote service not found: %s", service)
	}
	remoteAddr, _ := s.Router().Find(service)
	active := false
	for _, member := range members {
		if member.ServiceAddr == remoteAddr {
			active = true
			break
		}
	}
	if !active {
		s.Router().Delete(service)
		if route := h.currentNode.RemoteServiceRoute; route != nil {
			member := route(service, s, members)
			if member == nil {
				return fmt.Errorf("custom route not found: %s", service)
			}
			remoteAddr = member.ServiceAddr
			valid := false
			for _, candidate := range members {
				if candidate.ServiceAddr == remoteAddr {
					valid = true
				}
			}
			if !valid {
				return fmt.Errorf("custom route selected unavailable member: %s", remoteAddr)
			}
		} else {
			remoteAddr = members[rand.Intn(len(members))].ServiceAddr
		}
		s.Router().Bind(service, remoteAddr)
	}
	ctx, cancel := h.currentNode.rpcContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	pool, err := h.currentNode.rpcClient.getConnPoolContext(ctx, remoteAddr)
	if err != nil {
		return err
	}
	data := msg.Data
	if !noCopy {
		data = append([]byte(nil), data...)
	}
	gateAddr, sid := h.currentNode.ServiceAddr, s.ID()
	if ac, ok := s.NetworkEntity().(*acceptor); ok {
		gateAddr, sid = ac.gateAddr, ac.sid
	}
	client := clusterpb.NewMemberClient(pool.Get())
	switch msg.Type {
	case message.Request:
		_, err = client.HandleRequest(ctx, &clusterpb.RequestMessage{
			GateAddr: gateAddr, SessionId: sid, Id: msg.ID, Route: msg.Route, Data: data})
	case message.Notify:
		_, err = client.HandleNotify(ctx, &clusterpb.NotifyMessage{
			GateAddr: gateAddr, SessionId: sid, Route: msg.Route, Data: data})
	default:
		return fmt.Errorf("invalid remote message type: %v", msg.Type)
	}
	return err
}

func (h *LocalHandler) processMessage(agent *agent, msg *message.Message) error {
	var lastMid uint64
	switch msg.Type {
	case message.Request:
		lastMid = msg.ID
	case message.Notify:
		lastMid = 0
	default:
		return fmt.Errorf("invalid message type: %v", msg.Type)
	}

	handler, found := h.localHandlers[msg.Route]
	if !found {
		return h.remoteProcess(agent.ctx, agent.session, msg, false)
	} else {
		return h.localProcess(handler, lastMid, agent.session, msg)
	}
}

func (h *LocalHandler) handleWS(conn *websocket.Conn) {
	c, err := newWSConn(conn)
	if err != nil {
		conn.Close()
		log.Println(err)
		return
	}
	h.currentNode.serveConn(c)
}

func (h *LocalHandler) localProcess(handler *component.Handler, mid uint64, s *session.Session, msg *message.Message) error {
	s = s.WithRequest(s.Context(), mid)
	if pipe := h.pipeline; pipe != nil {
		if err := pipe.Inbound().Process(s, msg); err != nil {
			return err
		}
	}
	var data interface{}
	if handler.IsRawArg {
		data = append([]byte(nil), msg.Data...)
	} else {
		data = reflect.New(handler.Type.Elem()).Interface()
		if err := env.Serializer.Unmarshal(msg.Data, data); err != nil {
			return err
		}
	}
	index := strings.LastIndex(msg.Route, ".")
	if index <= 0 {
		return fmt.Errorf("invalid route: %s", msg.Route)
	}
	var local scheduler.LocalScheduler
	if service := h.localServices[msg.Route[:index]]; service != nil && service.SchedName != "" {
		var ok bool
		local, ok = s.Value(service.SchedName).(scheduler.LocalScheduler)
		if !ok {
			return fmt.Errorf("local scheduler not found: %s", service.SchedName)
		}
	}
	if h.currentNode != nil && !h.currentNode.beginTask() {
		return ErrNodeStopped
	}
	task := func() {
		if h.currentNode != nil {
			defer h.currentNode.tasks.Done()
		}
		if s.Context().Err() != nil {
			return
		}
		// Preserve LastMid on the base entity for compatibility. Responses use
		// the immutable ID on the request session instead.
		switch v := s.NetworkEntity().(type) {
		case *agent:
			atomic.StoreUint64(&v.lastMid, mid)
		case *acceptor:
			atomic.StoreUint64(&v.lastMid, mid)
		}
		args := []reflect.Value{handler.Receiver, reflect.ValueOf(s), reflect.ValueOf(data)}
		result := handler.Method.Func.Call(args)
		if err := result[0].Interface(); err != nil {
			log.Println("Handler error", msg.Route, err)
		}
	}
	if local != nil {
		local.Schedule(task)
		return nil
	}
	if err := scheduler.PushTask(task); err != nil {
		if h.currentNode != nil {
			h.currentNode.tasks.Done()
		}
		return err
	}
	return nil
}
