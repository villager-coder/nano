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
	"github.com/lonng/nano/internal/env"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"time"
)

var errRPCClientClosed = errors.New("rpc client is closed")

type connPool struct {
	once  sync.Once
	index uint32
	v     []*grpc.ClientConn // Immutable after publication, including during Close.
}
type pendingPool struct {
	ready chan struct{}
	pool  *connPool
	err   error
}
type rpcClient struct {
	sync.RWMutex
	isClosed bool
	pools    map[string]*connPool
	creating map[string]*pendingPool
	ctx      context.Context
	cancel   context.CancelFunc
}

func newConnArray(maxSize uint, addr string) (*connPool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return newConnArrayContext(ctx, maxSize, addr)
}
func newConnArrayContext(ctx context.Context, maxSize uint, addr string) (*connPool, error) {
	if maxSize == 0 {
		return nil, errors.New("empty connection pool")
	}
	a := &connPool{v: make([]*grpc.ClientConn, maxSize)}
	for i := range a.v {
		conn, err := grpc.DialContext(ctx, addr, env.GrpcOptions...)
		if err != nil {
			a.Close()
			return nil, err
		}
		a.v[i] = conn
	}
	return a, nil
}
func (a *connPool) Get() *grpc.ClientConn {
	return a.v[atomic.AddUint32(&a.index, 1)%uint32(len(a.v))]
}
func (a *connPool) Close() {
	a.once.Do(func() {
		for _, conn := range a.v {
			if conn != nil {
				conn.Close()
			}
		}
	})
}
func newRPCClient() *rpcClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &rpcClient{pools: make(map[string]*connPool), creating: make(map[string]*pendingPool), ctx: ctx, cancel: cancel}
}
func (c *rpcClient) getConnPool(addr string) (*connPool, error) {
	return c.getConnPoolContext(context.Background(), addr)
}
func (c *rpcClient) getConnPoolContext(ctx context.Context, addr string) (*connPool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.Lock()
	if c.isClosed {
		c.Unlock()
		return nil, errRPCClientClosed
	}
	if pool := c.pools[addr]; pool != nil {
		c.Unlock()
		return pool, nil
	}
	if pending := c.creating[addr]; pending != nil {
		c.Unlock()
		select {
		case <-pending.ready:
			return pending.pool, pending.err
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, errRPCClientClosed
		}
	}
	pending := &pendingPool{ready: make(chan struct{})}
	c.creating[addr] = pending
	c.Unlock()
	// Dial outside the registry lock; one creator per address, cancellable on close.
	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	stop := context.AfterFunc(c.ctx, cancel)
	pool, err := newConnArrayContext(dialCtx, 10, addr)
	stop()
	cancel()
	c.Lock()
	if c.isClosed {
		err = errRPCClientClosed
	}
	if err == nil {
		c.pools[addr] = pool
	} else if pool != nil {
		pool.Close()
		pool = nil
	}
	pending.pool, pending.err = pool, err
	delete(c.creating, addr)
	close(pending.ready)
	c.Unlock()
	return pool, err
}
func (c *rpcClient) closePool() {
	c.cancel()
	c.Lock()
	if c.isClosed {
		c.Unlock()
		return
	}
	c.isClosed = true
	pools := c.pools
	c.pools = make(map[string]*connPool)
	c.Unlock()
	for _, pool := range pools {
		pool.Close()
	}
}
