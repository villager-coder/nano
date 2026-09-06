package cluster

import (
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) {
	go scheduler.Sched()
	result := m.Run()
	scheduler.Close()
	os.Exit(result)
}
func await(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
	}
}
func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met")
		}
		runtime.Gosched()
	}
}

type queuedScheduler struct{ tasks []scheduler.Task }

func (q *queuedScheduler) Schedule(task scheduler.Task) { q.tasks = append(q.tasks, task) }

type lifecycleReceiver struct {
	received []string
	sessions []*session.Session
}

func (r *lifecycleReceiver) Handle(s *session.Session, data []byte) error {
	r.received = append(r.received, string(data))
	r.sessions = append(r.sessions, s)
	return nil
}
func rawHandler(r *lifecycleReceiver) *component.Handler {
	method, _ := reflect.TypeOf(r).MethodByName("Handle")
	return &component.Handler{Receiver: reflect.ValueOf(r), Method: method, IsRawArg: true}
}
func queuedHandler(a *agent, r *lifecycleReceiver) (*LocalHandler, *queuedScheduler) {
	h := NewHandler(nil, nil)
	h.localServices["test"] = &component.Service{SchedName: "queue"}
	h.localHandlers["test.Handle"] = rawHandler(r)
	q := &queuedScheduler{}
	a.session.Set("queue", q)
	return h, q
}
func testMessage(t *testing.T, payload string) []byte {
	t.Helper()
	data, err := (&message.Message{Type: message.Notify, Route: "test.Handle", Data: []byte(payload)}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func TestHandshakeStateMachine(t *testing.T) {
	old := env.HandshakeValidator
	t.Cleanup(func() { env.HandshakeValidator = old })
	var validated int
	env.HandshakeValidator = func(*session.Session, []byte) error { validated++; return nil }
	for _, first := range []packet.Type{packet.HandshakeAck, packet.Data, packet.Kick} {
		t.Run(string(rune('0'+first)), func(t *testing.T) {
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			a := newAgent(conn, nil, nil)
			h := NewHandler(nil, nil)
			if err := h.processPacket(a, &packet.Packet{Type: first}); err == nil {
				t.Fatal("accepted invalid first packet")
			}
			if a.status() == statusWorking {
				t.Fatal("bypassed handshake")
			}
		})
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	a := newAgent(conn, nil, nil)
	h := NewHandler(nil, nil)
	if err := h.processPacket(a, &packet.Packet{Type: packet.Handshake}); err != nil {
		t.Fatal(err)
	}
	control := <-a.chSend
	if len(control.packet) == 0 {
		t.Fatal("handshake bypassed write queue")
	}
	if err := h.processPacket(a, &packet.Packet{Type: packet.Handshake}); err == nil {
		t.Fatal("accepted duplicate handshake")
	}
	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err != nil {
		t.Fatal(err)
	}
	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err == nil {
		t.Fatal("accepted duplicate ACK")
	}
	if validated != 1 {
		t.Fatalf("validator calls: %d", validated)
	}
}
func TestRawPayloadOwnedAcrossDecode(t *testing.T) {
	decoder := codec.NewDecoder()
	frame := func(value string) []byte {
		data, err := codec.Encode(packet.Data, testMessage(t, value))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	first, err := decoder.Decode(frame("FIRST"))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := message.Decode(first[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	a := &agent{}
	a.session = session.New(a)
	r := &lifecycleReceiver{}
	h, q := queuedHandler(a, r)
	if err := h.localProcess(rawHandler(r), 1, a.session, msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := decoder.Decode(frame("OTHER")); err != nil {
			t.Fatal(err)
		}
	}
	q.tasks[0]()
	if r.received[0] != "FIRST" {
		t.Fatalf("raw data overwritten: %q", r.received[0])
	}
}
func TestAsyncResponsesKeepRequestID(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	a := newAgent(conn, nil, nil)
	r := &lifecycleReceiver{}
	h, q := queuedHandler(a, r)
	for _, mid := range []uint64{1, 2} {
		if err := h.localProcess(rawHandler(r), mid, a.session, &message.Message{Route: "test.Handle", Data: []byte{1}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, task := range q.tasks {
		task()
	}
	if err := r.sessions[1].Response([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := r.sessions[0].Response([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if got := (<-a.chSend).mid; got != 2 {
		t.Fatalf("second response ID=%d", got)
	}
	if got := (<-a.chSend).mid; got != 1 {
		t.Fatalf("first response ID=%d", got)
	}
}

type fastConn struct {
	net.Conn
	writes atomic.Int64
	done   chan struct{}
	target int64
}

func (c *fastConn) Write(b []byte) (int, error) {
	if c.writes.Add(1) == c.target {
		close(c.done)
	}
	return len(b), nil
}
func (*fastConn) SetWriteDeadline(time.Time) error { return nil }
func (*fastConn) Close() error                     { return nil }
func (*fastConn) RemoteAddr() net.Addr             { return &net.TCPAddr{Port: 1} }
func TestWritePumpDrainsBurst(t *testing.T) {
	c := &fastConn{done: make(chan struct{}), target: 10000}
	a := newAgent(c, nil, nil)
	finished := make(chan struct{})
	go func() { defer close(finished); a.write() }()
	defer func() { a.Close(); await(t, finished) }()
	for i := 0; i < 10000; i++ {
		select {
		case a.chSend <- pendingMessage{typ: message.Push, route: "test.Push", payload: []byte{1}}:
		case <-time.After(3 * time.Second):
			t.Fatal("write queue stalled")
		}
	}
	await(t, c.done)
}
func TestWriteTimeoutClosesSlowConnection(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	a := newAgent(conn, nil, nil)
	a.writeTimeout = 20 * time.Millisecond
	finished := make(chan struct{})
	go func() { defer close(finished); a.write() }()
	if err := a.Push("test.Push", []byte{1}); err != nil {
		t.Fatal(err)
	}
	await(t, finished)
	if a.status() != statusClosed {
		t.Fatal("slow socket was not closed")
	}
}
func TestDisconnectRemovesSession(t *testing.T) {
	n := &Node{sessions: make(map[int64]*session.Session)}
	n.cluster = newCluster(n)
	h := NewHandler(n, nil)
	conn, peer := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); h.handle(conn) }()
	peer.Close()
	await(t, done)
	n.mu.RLock()
	count := len(n.sessions)
	n.mu.RUnlock()
	if count != 0 {
		t.Fatalf("retained sessions: %d", count)
	}
}
func TestShutdownClosesTCPAndIdleWebsocket(t *testing.T) {
	for _, ws := range []bool{false, true} {
		t.Run(map[bool]string{false: "tcp", true: "websocket"}[ws], func(t *testing.T) {
			n := &Node{Options: Options{ClientAddr: "127.0.0.1:0", IsWebsocket: ws, ShutdownTimeout: time.Second}, ServiceAddr: "127.0.0.1:0"}
			if err := n.Startup(); err != nil {
				t.Fatal(err)
			}
			defer n.Shutdown()
			addr := n.clientListener.Addr().String()
			var conn net.Conn
			if ws {
				c, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/"+env.WSPath, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				conn = c.UnderlyingConn()
			} else {
				var err error
				conn, err = net.Dial("tcp", addr)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
			}
			eventually(t, func() bool { n.mu.RLock(); defer n.mu.RUnlock(); return len(n.sessions) == 1 })
			done := make(chan struct{})
			go func() { n.Shutdown(); n.Shutdown(); close(done) }()
			await(t, done)
			if c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
				c.Close()
				t.Fatal("listener survived shutdown")
			}
			conn.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := conn.Read(make([]byte, 1)); err == nil {
				t.Fatal("client connection survived shutdown")
			}
		})
	}
}
func TestRouteUpsertAndSnapshot(t *testing.T) {
	h := NewHandler(nil, nil)
	info := &clusterpb.MemberInfo{ServiceAddr: "one", Services: []string{"test", "old"}}
	h.addRemoteService(info)
	h.addRemoteService(info)
	before := h.findMembers("test")
	if len(before) != 1 {
		t.Fatal("duplicate route")
	}
	info.Services = []string{"test"}
	h.addRemoteService(info)
	if len(h.findMembers("old")) != 0 {
		t.Fatal("old service retained")
	}
	before[0].Services[0] = "mutated"
	if h.findMembers("test")[0].Services[0] != "test" {
		t.Fatal("snapshot aliases routing table")
	}
	h.delMember("one")
	if len(h.findMembers("test")) != 0 {
		t.Fatal("deleted member retained")
	}
}
func TestConcurrentSessionCreation(t *testing.T) {
	n := &Node{sessions: make(map[int64]*session.Session), rpcClient: newRPCClient()}
	n.handler = NewHandler(n, nil)
	defer n.rpcClient.closePool()
	var wg sync.WaitGroup
	results := make(chan *session.Session, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := n.findOrCreateSession(7, "127.0.0.1:1")
			if err != nil {
				t.Error(err)
				return
			}
			results <- s
		}()
	}
	wg.Wait()
	close(results)
	var first *session.Session
	for s := range results {
		if first == nil {
			first = s
		}
		if s != first {
			t.Fatal("multiple session instances for same SID")
		}
	}
	if first == nil {
		t.Fatal("no session created")
	}
	first.NetworkEntity().(*acceptor).closeLocal()
}

type blockingGate struct{ clusterpb.MemberClient }

func (*blockingGate) HandlePush(ctx context.Context, _ *clusterpb.PushMessage, _ ...grpc.CallOption) (*clusterpb.MemberHandleResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestRPCDeadlineAndCancellation(t *testing.T) {
	n := &Node{Options: Options{RPCTimeout: 20 * time.Millisecond}}
	a := &acceptor{node: n, gateClient: &blockingGate{}}
	if err := a.Push("test.Push", []byte{1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.PushContext(ctx, "test.Push", []byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
func TestAdmittedRequestSurvivesAcknowledgment(t *testing.T) {
	n := &Node{Options: Options{RPCTimeout: time.Second}}
	incoming, cancelIncoming := context.WithTimeout(context.Background(), 500*time.Millisecond)
	lifetime, closeSession := context.WithCancel(context.Background())
	request, cancelRequest := n.admittedContext(incoming, lifetime)
	defer cancelRequest()
	cancelIncoming()
	if request.Err() != nil {
		t.Fatal("admitted request canceled by transport ACK")
	}
	deadline, _ := incoming.Deadline()
	actual, _ := request.Deadline()
	if !actual.Equal(deadline) {
		t.Fatal("incoming deadline was lost")
	}
	closeSession()
	await(t, request.Done())
}
func TestRPCReportsMissingRoute(t *testing.T) {
	n := &Node{}
	h := NewHandler(n, nil)
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	a := newAgent(conn, nil, h.remoteProcess)
	if err := a.RPC("missing.Handle", []byte{1}); err == nil {
		t.Fatal("routing error was swallowed")
	}
}

type stalledMaster struct {
	clusterpb.UnimplementedMasterServer
	entered chan struct{}
	once    sync.Once
}

func (m *stalledMaster) Register(ctx context.Context, _ *clusterpb.RegisterRequest) (*clusterpb.RegisterResponse, error) {
	m.once.Do(func() { close(m.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestShutdownCancelsStartupRegistration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	master := &stalledMaster{entered: make(chan struct{})}
	clusterpb.RegisterMasterServer(server, master)
	serveDone := make(chan struct{})
	go func() { defer close(serveDone); server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); await(t, serveDone) }()
	n := &Node{Options: Options{AdvertiseAddr: listener.Addr().String(), RPCTimeout: time.Second, ShutdownTimeout: time.Second}, ServiceAddr: "127.0.0.1:0"}
	result := make(chan error, 1)
	go func() { result <- n.Startup() }()
	await(t, master.entered)
	done := make(chan struct{})
	go func() { n.Shutdown(); close(done) }()
	await(t, done)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("startup result: %v", err)
	}
	if err := n.Startup(); err == nil {
		t.Fatal("restarted a stopped node")
	}
	if _, err := n.rpcClient.getConnPool("127.0.0.1:1"); err == nil {
		t.Fatal("shutdown retained open RPC client")
	}
}
func TestClientListenerBindFailureReturned(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	n := &Node{Options: Options{ClientAddr: listener.Addr().String()}, ServiceAddr: "127.0.0.1:0"}
	if err := n.Startup(); err == nil {
		n.Shutdown()
		t.Fatal("Startup hid the bind failure")
	}
	n.Shutdown()
}
func TestPoolCloseCancelsInflightDial(t *testing.T) {
	old := env.GrpcOptions
	defer func() { env.GrpcOptions = old }()
	entered := make(chan struct{})
	var once sync.Once
	env.GrpcOptions = append(append([]grpc.DialOption(nil), old...), grpc.WithBlock(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	client := newRPCClient()
	done := make(chan struct{})
	var dialErr error
	go func() { defer close(done); _, dialErr = client.getConnPool("blocked.test:1234") }()
	await(t, entered)
	client.closePool()
	await(t, done)
	if !errors.Is(dialErr, errRPCClientClosed) {
		t.Fatalf("dial survived close: %v", dialErr)
	}
	if _, err := client.getConnPool("blocked.test:1234"); !errors.Is(err, errRPCClientClosed) {
		t.Fatal(err)
	}
}
func TestConcurrentRoutingSnapshots(t *testing.T) {
	h := NewHandler(nil, nil)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				h.addRemoteService(&clusterpb.MemberInfo{ServiceAddr: "node", Services: []string{"test"}})
				for _, m := range h.findMembers("test") {
					m.Services[0] = "private"
				}
				h.delMember("node")
			}
		}()
	}
	wg.Wait()
}

type slowMember struct {
	clusterpb.UnimplementedMemberServer
}

func (*slowMember) HandleNotify(ctx context.Context, _ *clusterpb.NotifyMessage) (*clusterpb.MemberHandleResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestRemoteRPCDeadlineAndStaleBinding(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	clusterpb.RegisterMemberServer(server, &slowMember{})
	done := make(chan struct{})
	go func() { defer close(done); server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); await(t, done) }()
	n := &Node{Options: Options{RPCTimeout: 100 * time.Millisecond}, rpcClient: newRPCClient()}
	defer n.rpcClient.closePool()
	h := NewHandler(n, nil)
	h.addRemoteService(&clusterpb.MemberInfo{ServiceAddr: listener.Addr().String(), Services: []string{"slow"}})
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	a := newAgent(conn, nil, h.remoteProcess)
	a.session.Router().Bind("slow", "retired:1234")
	if err := a.RPC("slow.Handle", []byte{1}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("remote deadline: %v", err)
	}
	addr, _ := a.session.Router().Find("slow")
	if addr != listener.Addr().String() {
		t.Fatalf("stale binding retained: %s", addr)
	}
}
