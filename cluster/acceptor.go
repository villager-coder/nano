package cluster

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/mock"
	"github.com/lonng/nano/session"
)

type acceptor struct {
	node       *Node
	ctx        context.Context
	cancel     context.CancelFunc
	closed     int32
	sid        int64
	gateClient clusterpb.MemberClient
	session    *session.Session
	lastMid    uint64
	rpcHandler rpcHandler
	gateAddr   string
}

// Push implements the session.NetworkEntity interface
func (a *acceptor) Push(route string, v interface{}) error {
	return a.PushContext(a.ctx, route, v)
}
func (a *acceptor) PushContext(ctx context.Context, route string, v interface{}) error {
	ctx, cancel := a.callContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	// TODO: buffer
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	request := &clusterpb.PushMessage{
		SessionId: a.sid,
		Route:     route,
		Data:      data,
	}
	_, err = a.gateClient.HandlePush(ctx, request)
	return err
}

// RPC implements the session.NetworkEntity interface
func (a *acceptor) RPC(route string, v interface{}) error {
	return a.RPCContext(a.ctx, route, v)
}
func (a *acceptor) RPCContext(ctx context.Context, route string, v interface{}) error {
	ctx, cancel := a.callContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
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
	return a.rpcHandler(ctx, a.session, msg, true)
}

// LastMid implements the session.NetworkEntity interface
func (a *acceptor) LastMid() uint64 {
	return atomic.LoadUint64(&a.lastMid)
}

// Response implements the session.NetworkEntity interface
func (a *acceptor) Response(v interface{}) error {
	return a.ResponseMid(a.LastMid(), v)
}

// ResponseMid implements the session.NetworkEntity interface
func (a *acceptor) ResponseMid(mid uint64, v interface{}) error {
	return a.ResponseMidContext(a.ctx, mid, v)
}
func (a *acceptor) ResponseMidContext(ctx context.Context, mid uint64, v interface{}) error {
	if mid == 0 {
		return ErrSessionOnNotify
	}
	ctx, cancel := a.callContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	// TODO: buffer
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	request := &clusterpb.ResponseMessage{
		SessionId: a.sid,
		Id:        mid,
		Data:      data,
	}
	_, err = a.gateClient.HandleResponse(ctx, request)
	return err
}

// Close implements the session.NetworkEntity interface
func (a *acceptor) Close() error {
	ctx, cancel := a.callContext(a.ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	// TODO: buffer
	request := &clusterpb.CloseSessionRequest{
		SessionId: a.sid,
	}
	_, err := a.gateClient.CloseSession(ctx, request)
	return err
}

// RemoteAddr implements the session.NetworkEntity interface
func (*acceptor) RemoteAddr() net.Addr {
	return mock.NetAddr{}
}

func (a *acceptor) callContext(parent context.Context) (context.Context, context.CancelFunc) {
	node := a.node
	if node == nil {
		node = &Node{}
	}
	ctx, cancel := node.rpcContext(parent)
	if a.ctx != nil {
		stop := context.AfterFunc(a.ctx, cancel)
		context.AfterFunc(ctx, func() { stop() })
		if a.ctx.Err() != nil {
			cancel()
		}
		return ctx, func() { stop(); cancel() }
	}
	return ctx, cancel
}

func (a *acceptor) closeLocal() {
	if atomic.CompareAndSwapInt32(&a.closed, 0, 1) && a.cancel != nil {
		a.cancel()
	}
}
