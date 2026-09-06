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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
	"google.golang.org/grpc"
)

var ErrNodeStopped = errors.New("node stopped")

type Options struct {
	Pipeline           pipeline.Pipeline
	IsMaster           bool
	AdvertiseAddr      string
	RetryInterval      time.Duration
	ClientAddr         string
	Components         *component.Components
	Label              string
	IsWebsocket        bool
	TSLCertificate     string
	TSLKey             string
	UnregisterCallback func(Member)
	RemoteServiceRoute CustomerRemoteServiceRoute
	RPCTimeout         time.Duration
	WriteTimeout       time.Duration
	ShutdownTimeout    time.Duration
}

type Node struct {
	started     bool
	startupDone chan struct{}
	Options
	ServiceAddr    string
	cluster        *cluster
	handler        *LocalHandler
	server         *grpc.Server
	rpcClient      *rpcClient
	clientListener net.Listener
	httpServer     *http.Server

	mu           sync.RWMutex
	sessions     map[int64]*session.Session
	stopping     bool
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownOnce sync.Once
	connections  sync.WaitGroup
	workers      sync.WaitGroup
	tasks        sync.WaitGroup
	initialized  bool
}

// Startup binds the listening sockets before reporting success.
func (n *Node) Startup() error { return n.StartupContext(context.Background()) }

// StartupContext allows cancellation while registering with the master.
func (n *Node) StartupContext(parent context.Context) (err error) {
	n.mu.Lock()
	if n.started || n.stopping {
		n.mu.Unlock()
		return errors.New("node already started or stopped")
	}
	n.started = true
	n.startupDone = make(chan struct{})
	n.ctx, n.cancel = context.WithCancel(parent)
	n.mu.Unlock()
	defer func() {
		close(n.startupDone)
		if err != nil {
			n.Shutdown()
		}
	}()

	if n.ServiceAddr == "" {
		return errors.New("service address cannot be empty")
	}
	if n.Components == nil {
		n.Components = &component.Components{}
	}
	if n.RetryInterval <= 0 {
		n.RetryInterval = 3 * time.Second
	}
	n.sessions = make(map[int64]*session.Session)
	n.cluster = newCluster(n)
	n.handler = NewHandler(n, n.Pipeline)
	components := n.Components.List()
	for _, c := range components {
		if err = n.handler.register(c.Comp, c.Opts); err != nil {
			return err
		}
	}
	if err = n.initNode(); err != nil {
		return err
	}
	for _, c := range components {
		c.Comp.Init()
	}
	for _, c := range components {
		c.Comp.AfterInit()
	}
	n.initialized = true
	if n.ClientAddr != "" {
		if err = n.startClientListener(); err != nil {
			return err
		}
	}
	if n.IsMaster {
		n.cluster.checkMemberHeartbeat()
	} else if n.AdvertiseAddr != "" {
		n.keepalive()
	}
	return nil
}

func (n *Node) Handler() *LocalHandler { return n.handler }
func (n *Node) context() context.Context {
	if n.ctx != nil {
		return n.ctx
	}
	return context.Background()
}
func (n *Node) writeTimeout() time.Duration {
	if n.WriteTimeout > 0 {
		return n.WriteTimeout
	}
	return 5 * time.Second
}
func (n *Node) rpcContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := n.RPCTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	stop := context.AfterFunc(n.context(), cancel)
	if n.context().Err() != nil {
		cancel()
	}
	context.AfterFunc(ctx, func() { stop() })
	return ctx, func() { stop(); cancel() }
}

// A successful HandleRequest/Notify acknowledges admission, not completion.
// Its transport context ends at that ACK. Preserve its deadline and values for
// queued work, and continue to honor the node's lifetime.
func (n *Node) admittedContext(incoming context.Context, lifetime context.Context) (context.Context, context.CancelFunc) {
	parent := context.WithoutCancel(incoming)
	finishParent := func() {}
	if deadline, ok := incoming.Deadline(); ok {
		parent, finishParent = context.WithDeadline(parent, deadline)
	}
	ctx, cancel := n.rpcContext(parent)
	stop := context.AfterFunc(lifetime, cancel)
	context.AfterFunc(ctx, func() { stop(); finishParent() })
	if lifetime.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel(); finishParent() }
}

func (n *Node) initNode() error {
	if !n.IsMaster && n.AdvertiseAddr == "" {
		return nil
	}
	listener, err := net.Listen("tcp", n.ServiceAddr)
	if err != nil {
		return err
	}
	if strings.HasSuffix(n.ServiceAddr, ":0") {
		n.ServiceAddr = listener.Addr().String()
	}
	n.server = grpc.NewServer()
	n.rpcClient = newRPCClient()
	clusterpb.RegisterMemberServer(n.server, n)
	if n.IsMaster {
		clusterpb.RegisterMasterServer(n.server, n.cluster)
		n.cluster.members = []*Member{{isMaster: true, memberInfo: n.memberInfo()}}
		n.cluster.setRpcClient(n.rpcClient)
	}
	// All services must be registered before Serve starts.
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		defer listener.Close()
		if err := n.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Println("RPC server stopped", err)
		}
	}()
	if n.IsMaster {
		return nil
	}
	pool, err := n.rpcClient.getConnPoolContext(n.context(), n.AdvertiseAddr)
	if err != nil {
		return err
	}
	client := clusterpb.NewMasterClient(pool.Get())
	for {
		ctx, cancel := n.rpcContext(n.context())
		response, err := client.Register(ctx, &clusterpb.RegisterRequest{MemberInfo: n.memberInfo()})
		cancel()
		if err == nil {
			n.handler.initRemoteService(response.Members)
			n.cluster.initMembers(response.Members)
			return nil
		}
		log.Println("Register failed", err)
		timer := time.NewTimer(n.RetryInterval)
		select {
		case <-n.context().Done():
			timer.Stop()
			return n.context().Err()
		case <-timer.C:
		}
	}
}

func (n *Node) memberInfo() *clusterpb.MemberInfo {
	return &clusterpb.MemberInfo{Label: n.Label, ServiceAddr: n.ServiceAddr, Services: n.handler.LocalService()}
}

func (n *Node) startClientListener() error {
	listener, err := net.Listen("tcp", n.ClientAddr)
	if err != nil {
		return err
	}
	if n.IsWebsocket && n.TSLCertificate != "" {
		certificate, err := tls.LoadX509KeyPair(n.TSLCertificate, n.TSLKey)
		if err != nil {
			listener.Close()
			return err
		}
		listener = tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	}
	n.clientListener = listener
	if strings.HasSuffix(n.ClientAddr, ":0") {
		n.ClientAddr = listener.Addr().String()
	}
	if n.IsWebsocket {
		mux := http.NewServeMux()
		upgrader := websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 1024, CheckOrigin: env.CheckOrigin}
		mux.HandleFunc("/"+strings.TrimPrefix(env.WSPath, "/"), func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			n.handler.handleWS(conn)
		})
		n.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: n.writeTimeout()}
	}
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		if n.httpServer != nil {
			if err := n.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				log.Println(err)
			}
			return
		}
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				select {
				case <-n.context().Done():
					return
				default:
				}
				log.Println(err)
				return
			}
			n.serveConn(conn)
		}
	}()
	return nil
}

func (n *Node) serveConn(conn net.Conn) {
	n.mu.Lock()
	if n.stopping {
		n.mu.Unlock()
		conn.Close()
		return
	}
	n.connections.Add(1)
	n.mu.Unlock()
	go func() { defer n.connections.Done(); n.handler.handle(conn) }()
}

func (n *Node) beginTask() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping {
		return false
	}
	n.tasks.Add(1)
	return true
}

func waitGroup(ctx context.Context, wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Shutdown is idempotent. Network/RPC waits share a bounded shutdown budget;
// application hooks remain responsible for terminating their own work.
func (n *Node) Shutdown() {
	n.shutdownOnce.Do(func() {
		n.mu.Lock()
		n.stopping = true
		cancelStartup, startupDone := n.cancel, n.startupDone
		n.mu.Unlock()
		if cancelStartup != nil {
			cancelStartup()
		}
		if startupDone != nil {
			<-startupDone
		}

		timeout := n.ShutdownTimeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		n.mu.Lock()
		n.stopping = true
		sessions := make([]*session.Session, 0, len(n.sessions))
		for _, s := range n.sessions {
			sessions = append(sessions, s)
		}
		n.sessions = make(map[int64]*session.Session)
		n.mu.Unlock()
		if n.clientListener != nil {
			n.clientListener.Close()
		}
		if n.httpServer != nil {
			n.httpServer.Close()
		}
		if n.cancel != nil {
			n.cancel()
		}
		for _, s := range sessions {
			if ac, ok := s.NetworkEntity().(*acceptor); ok {
				ac.closeLocal()
				n.finalizeSession(s)
			} else {
				s.Close()
			}
		}
		if !n.IsMaster && n.AdvertiseAddr != "" && n.rpcClient != nil {
			if pool, err := n.rpcClient.getConnPoolContext(ctx, n.AdvertiseAddr); err == nil {
				_, err = clusterpb.NewMasterClient(pool.Get()).Unregister(ctx, &clusterpb.UnregisterRequest{ServiceAddr: n.ServiceAddr})
				if err != nil {
					log.Println("Unregister failed", err)
				}
			}
		}
		if n.server != nil {
			stopped := make(chan struct{})
			go func() { n.server.GracefulStop(); close(stopped) }()
			select {
			case <-stopped:
			case <-ctx.Done():
				n.server.Stop()
				<-stopped
			}
		}
		if n.rpcClient != nil {
			n.rpcClient.closePool()
		}
		waitGroup(ctx, &n.connections)
		waitGroup(ctx, &n.workers)
		waitGroup(ctx, &n.tasks)
		if n.initialized {
			components := n.Components.List()
			for i := len(components) - 1; i >= 0; i-- {
				components[i].Comp.BeforeShutdown()
			}
			for i := len(components) - 1; i >= 0; i-- {
				components[i].Comp.Shutdown()
			}
		}
	})
}

func (n *Node) storeSession(s *session.Session) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping {
		return false
	}
	n.sessions[s.ID()] = s
	return true
}
func (n *Node) removeSession(sid int64, expected *session.Session) {
	n.mu.Lock()
	if n.sessions[sid] == expected {
		delete(n.sessions, sid)
	}
	n.mu.Unlock()
}
func (n *Node) findSession(sid int64) *session.Session {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.sessions[sid]
}
func (n *Node) findOrCreateSession(sid int64, gateAddr string, contexts ...context.Context) (*session.Session, error) {
	n.mu.RLock()
	s, stopped := n.sessions[sid], n.stopping
	n.mu.RUnlock()
	if stopped {
		return nil, ErrNodeStopped
	}
	if s != nil {
		return s, nil
	}
	parent := n.context()
	if len(contexts) > 0 {
		parent = contexts[0]
	}
	conns, err := n.rpcClient.getConnPoolContext(parent, gateAddr)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping {
		return nil, ErrNodeStopped
	}
	if s = n.sessions[sid]; s != nil {
		return s, nil
	}
	ctx, cancel := context.WithCancel(n.context())
	ac := &acceptor{sid: sid, gateClient: clusterpb.NewMemberClient(conns.Get()),
		rpcHandler: n.handler.remoteProcess, gateAddr: gateAddr, node: n, ctx: ctx, cancel: cancel}
	s = session.NewWithContext(ctx, ac)
	ac.session = s
	n.sessions[sid] = s
	return s, nil
}

func (n *Node) closeRemoteSessions(gateAddr string) {
	n.mu.Lock()
	var removed []*session.Session
	for sid, s := range n.sessions {
		if ac, ok := s.NetworkEntity().(*acceptor); ok && ac.gateAddr == gateAddr {
			delete(n.sessions, sid)
			ac.closeLocal()
			removed = append(removed, s)
		}
	}
	n.mu.Unlock()
	for _, s := range removed {
		n.finalizeSession(s)
	}
}

func (n *Node) HandleRequest(ctx context.Context, req *clusterpb.RequestMessage) (*clusterpb.MemberHandleResponse, error) {
	handler, found := n.handler.localHandlers[req.Route]
	if !found {
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}
	s, err := n.findOrCreateSession(req.SessionId, req.GateAddr, ctx)
	if err != nil {
		return nil, err
	}
	msg := &message.Message{
		Type:  message.Request,
		ID:    req.Id,
		Route: req.Route,
		Data:  req.Data,
	}
	requestCtx, cancel := n.admittedContext(ctx, s.Context())
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, err
	}
	err = n.handler.localProcess(handler, req.Id, s.WithRequest(requestCtx, req.Id), msg)
	if err != nil {
		cancel()
	}
	return &clusterpb.MemberHandleResponse{}, err
}

func (n *Node) HandleNotify(ctx context.Context, req *clusterpb.NotifyMessage) (*clusterpb.MemberHandleResponse, error) {
	handler, found := n.handler.localHandlers[req.Route]
	if !found {
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}
	s, err := n.findOrCreateSession(req.SessionId, req.GateAddr, ctx)
	if err != nil {
		return nil, err
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: req.Route,
		Data:  req.Data,
	}
	requestCtx, cancel := n.admittedContext(ctx, s.Context())
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, err
	}
	err = n.handler.localProcess(handler, 0, s.WithRequest(requestCtx, 0), msg)
	if err != nil {
		cancel()
	}
	return &clusterpb.MemberHandleResponse{}, err
}

func (n *Node) HandlePush(ctx context.Context, req *clusterpb.PushMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, fmt.Errorf("session not found: %v", req.SessionId)
	}
	return &clusterpb.MemberHandleResponse{}, s.PushContext(ctx, req.Route, req.Data)
}

func (n *Node) HandleResponse(ctx context.Context, req *clusterpb.ResponseMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, fmt.Errorf("session not found: %v", req.SessionId)
	}
	return &clusterpb.MemberHandleResponse{}, s.WithRequest(ctx, req.Id).Response(req.Data)
}

func (n *Node) NewMember(_ context.Context, req *clusterpb.NewMemberRequest) (*clusterpb.NewMemberResponse, error) {
	n.handler.addRemoteService(req.MemberInfo)
	n.cluster.addMember(req.MemberInfo)
	return &clusterpb.NewMemberResponse{}, nil
}

func (n *Node) DelMember(_ context.Context, req *clusterpb.DelMemberRequest) (*clusterpb.DelMemberResponse, error) {
	log.Println("DelMember member", req.String())
	n.handler.delMember(req.ServiceAddr)
	n.cluster.delMember(req.ServiceAddr)
	n.closeRemoteSessions(req.ServiceAddr)
	return &clusterpb.DelMemberResponse{}, nil
}

// SessionClosed implements the MemberServer interface
func (n *Node) SessionClosed(_ context.Context, req *clusterpb.SessionClosedRequest) (*clusterpb.SessionClosedResponse, error) {
	n.mu.Lock()
	s, found := n.sessions[req.SessionId]
	delete(n.sessions, req.SessionId)
	n.mu.Unlock()
	if found {
		if ac, ok := s.NetworkEntity().(*acceptor); ok {
			ac.closeLocal()
		}
		n.finalizeSession(s)
	}
	return &clusterpb.SessionClosedResponse{}, nil
}

// CloseSession implements the MemberServer interface
func (n *Node) CloseSession(_ context.Context, req *clusterpb.CloseSessionRequest) (*clusterpb.CloseSessionResponse, error) {
	n.mu.Lock()
	s, found := n.sessions[req.SessionId]
	delete(n.sessions, req.SessionId)
	n.mu.Unlock()
	if found {
		s.Close()
	}
	return &clusterpb.CloseSessionResponse{}, nil
}

func (n *Node) keepalive() {
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		ticker := time.NewTicker(env.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-n.context().Done():
				return
			case <-ticker.C:
				pool, err := n.rpcClient.getConnPoolContext(n.context(), n.AdvertiseAddr)
				if err != nil {
					log.Println(err)
					continue
				}
				ctx, cancel := n.rpcContext(n.context())
				_, err = clusterpb.NewMasterClient(pool.Get()).Heartbeat(ctx, &clusterpb.HeartbeatRequest{MemberInfo: n.memberInfo()})
				cancel()
				if err != nil {
					log.Println("Heartbeat failed", err)
				}
			}
		}
	}()
}

func (n *Node) finalizeSession(s *session.Session) {
	n.tasks.Add(1)
	if err := scheduler.PushFinalizer(func() {
		defer n.tasks.Done()
		session.Lifetime.Close(s)
	}); err != nil {
		n.tasks.Done()
		log.Println(err)
	}
}
