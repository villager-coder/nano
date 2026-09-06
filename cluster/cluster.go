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
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

type cluster struct {
	currentNode *Node
	rpcClient   *rpcClient
	operations  sync.Mutex
	mu          sync.RWMutex
	members     []*Member
}

func cloneMemberInfo(info *clusterpb.MemberInfo) *clusterpb.MemberInfo {
	if info == nil {
		return nil
	}
	return proto.Clone(info).(*clusterpb.MemberInfo)
}
func newCluster(n *Node) *cluster { return &cluster{currentNode: n} }
func (c *cluster) snapshot() []*Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]*Member, 0, len(c.members))
	for _, m := range c.members {
		result = append(result, &Member{isMaster: m.isMaster, memberInfo: cloneMemberInfo(m.memberInfo), lastHeartbeatAt: m.lastHeartbeatAt})
	}
	return result
}

// Control-plane operations are serialized, but never hold the table lock over RPC.
func (c *cluster) Register(ctx context.Context, req *clusterpb.RegisterRequest) (*clusterpb.RegisterResponse, error) {
	if req.GetMemberInfo().GetServiceAddr() == "" {
		return nil, ErrInvalidRegisterReq
	}
	c.operations.Lock()
	defer c.operations.Unlock()
	ctx, cancel := c.currentNode.rpcContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info := cloneMemberInfo(req.MemberInfo)
	c.addMember(info)
	c.currentNode.handler.addRemoteService(info)
	resp := &clusterpb.RegisterResponse{}
	var notifyErr error
	for _, m := range c.snapshot() {
		if m.memberInfo.ServiceAddr == info.ServiceAddr {
			continue
		}
		resp.Members = append(resp.Members, m.memberInfo)
		if m.isMaster {
			continue
		}
		pool, err := c.rpcClient.getConnPoolContext(ctx, m.memberInfo.ServiceAddr)
		if err == nil {
			_, err = clusterpb.NewMemberClient(pool.Get()).NewMember(ctx, &clusterpb.NewMemberRequest{MemberInfo: info})
		}
		if err != nil {
			notifyErr = err
			log.Println("Member notification failed", err)
		}
	}
	if notifyErr != nil {
		return nil, notifyErr
	}
	return resp, nil
}
func (c *cluster) Unregister(ctx context.Context, req *clusterpb.UnregisterRequest) (*clusterpb.UnregisterResponse, error) {
	if req.GetServiceAddr() == "" {
		return nil, ErrInvalidRegisterReq
	}
	c.operations.Lock()
	defer c.operations.Unlock()
	return c.unregister(ctx, req.ServiceAddr)
}
func (c *cluster) unregister(ctx context.Context, addr string) (*clusterpb.UnregisterResponse, error) {
	// Local removal cannot depend on the health of other members.
	removed := c.delMember(addr)
	c.currentNode.handler.delMember(addr)
	c.currentNode.closeRemoteSessions(addr)
	ctx, cancel := c.currentNode.rpcContext(ctx)
	defer cancel()
	var notifyErr error
	for _, m := range c.snapshot() {
		if m.memberInfo.ServiceAddr == c.currentNode.ServiceAddr {
			continue
		}
		pool, err := c.rpcClient.getConnPoolContext(ctx, m.memberInfo.ServiceAddr)
		if err == nil {
			_, err = clusterpb.NewMemberClient(pool.Get()).DelMember(ctx, &clusterpb.DelMemberRequest{ServiceAddr: addr})
		}
		if err != nil {
			notifyErr = err
			log.Println("Member removal notification failed", err)
		}
	}
	if removed != nil && c.currentNode.UnregisterCallback != nil {
		c.currentNode.UnregisterCallback(*removed)
	}
	return &clusterpb.UnregisterResponse{}, notifyErr
}
func (c *cluster) Heartbeat(ctx context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	if req.GetMemberInfo().GetServiceAddr() == "" {
		return nil, ErrInvalidRegisterReq
	}
	c.operations.Lock()
	c.mu.Lock()
	found := false
	for _, m := range c.members {
		if m.memberInfo.ServiceAddr == req.MemberInfo.ServiceAddr {
			m.lastHeartbeatAt = time.Now()
			found = true
			break
		}
	}
	c.mu.Unlock()
	c.operations.Unlock()
	if !found {
		if _, err := c.Register(ctx, &clusterpb.RegisterRequest{MemberInfo: req.MemberInfo}); err != nil {
			return nil, err
		}
	}
	return &clusterpb.HeartbeatResponse{}, nil
}
func (c *cluster) checkMemberHeartbeat() {
	c.currentNode.workers.Add(1)
	go func() {
		defer c.currentNode.workers.Done()
		ticker := time.NewTicker(env.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-c.currentNode.context().Done():
				return
			case <-ticker.C:
				c.operations.Lock()
				for _, m := range c.snapshot() {
					if !m.isMaster && time.Since(m.lastHeartbeatAt) > 4*env.Heartbeat {
						if _, err := c.unregister(c.currentNode.context(), m.memberInfo.ServiceAddr); err != nil {
							log.Println(err)
						}
					}
				}
				c.operations.Unlock()
			}
		}
	}()
}
func (c *cluster) setRpcClient(client *rpcClient) { c.rpcClient = client }
func (c *cluster) remoteAddrs() []string {
	members := c.snapshot()
	addrs := make([]string, 0, len(members))
	for _, m := range members {
		addrs = append(addrs, m.memberInfo.ServiceAddr)
	}
	return addrs
}
func (c *cluster) initMembers(members []*clusterpb.MemberInfo) {
	for _, info := range members {
		c.addMember(info)
	}
}
func (c *cluster) addMember(info *clusterpb.MemberInfo) {
	if info == nil || info.ServiceAddr == "" {
		return
	}
	info = cloneMemberInfo(info)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.members {
		if m.memberInfo.ServiceAddr == info.ServiceAddr {
			m.memberInfo = info
			m.lastHeartbeatAt = time.Now()
			return
		}
	}
	c.members = append(c.members, &Member{memberInfo: info, lastHeartbeatAt: time.Now()})
}
func (c *cluster) delMember(addr string) *Member {
	c.mu.Lock()
	defer c.mu.Unlock()
	var removed *Member
	kept := make([]*Member, 0, len(c.members))
	for _, m := range c.members {
		if m.memberInfo.ServiceAddr == addr {
			removed = &Member{isMaster: m.isMaster, memberInfo: cloneMemberInfo(m.memberInfo), lastHeartbeatAt: m.lastHeartbeatAt}
		} else {
			kept = append(kept, m)
		}
	}
	c.members = kept
	return removed
}
