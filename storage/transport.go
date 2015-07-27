// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	gorpc "net/rpc"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
)

// NodeTransport directs RPCs to storage nodes by Node ID.
type NodeTransport interface {
	// Send asynchronously invokes the specified RPC method with args as
	// arguments using nodeID to lookup the address of the destination
	// node. The done channel can be used to asynchronously send the RPC
	// and receive notifications of call completion. If nil, then the RPCs
	// are sent synchronously and the result of the RPC returned.
	Send(nodeID proto.NodeID, method string, args, reply interface{}, done chan *gorpc.Call) error
}

// NewNodeTransport creates a new gossip-based RPC NodeTransport with
// specified gossip and rpc server.
func NewNodeTransport(gossip *gossip.Gossip, context *rpc.Context) NodeTransport {
	return &rpcNodeTransport{
		Gossip:  gossip,
		Context: context,
	}
}

// rpcNodeTransport directs RPCs to storage nodes by Node ID using the
// gossip network to map from ID to address.
//
// rpcNodeTransport is used by RaftTransport when sending Raft messages
// and also directly by leader range replicas, as in the case of
// garbage collecting a range after rebalancing. These requests do not
// go through the distributed sender because there is no need to map
// from key to node/store/range, and there is only one acceptable
// target replica for the RPC.
type rpcNodeTransport struct {
	Gossip  *gossip.Gossip
	Context *rpc.Context
}

// Send implements the Transport interface.
func (t *rpcNodeTransport) Send(nodeID proto.NodeID, method string, args, reply interface{}, done chan *gorpc.Call) error {
	if t == nil {
		return util.Errorf("unable to send %s RPC on nil transport", method)
	}
	addr, err := t.Gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return util.Errorf("could not get address for node %d: %s", nodeID, err)
	}
	client := rpc.NewClient(addr, t.Context)
	select {
	case <-t.Context.Stopper.ShouldStop():
		return util.Errorf("server is being stopped; abandoning RPC %s", method)
	case <-client.Closed:
		return util.Errorf("client for node %d failed to connect", nodeID)
	case <-client.Healthy():
	}
	if done == nil {
		return client.Call(method, args, reply)
	}
	client.Go(method, args, reply, done)
	return nil
}
