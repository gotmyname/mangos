// Copyright 2016 The Mangos Authors
// Copyright 2017 gotmyname@outlook.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use file except in compliance with the License.
// You may obtain a copy of the license at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package client implements the CLIENT protocol. which is the client side of
// the client/server pattern.  (SERVER is the server.)
package client

import (
	"sync"
	"time"

	"github.com/go-mangos/mangos"
)

type client struct {
	sock mangos.ProtocolSocket
	server *serverEp
	raw  bool
	w    mangos.Waiter
	sync.Mutex
}

type serverEp struct {
	ep mangos.Endpoint
	cq chan struct{}
}

func (x *client) Init(sock mangos.ProtocolSocket) {
	x.sock = sock
	x.w.Init()
}

func (x *client) Shutdown(expire time.Time) {
	x.w.WaitAbsTimeout(expire)
}

func (x *client) sender(ep *serverEp) {

	defer x.w.Done()
	sq := x.sock.SendChannel()
	cq := x.sock.CloseChannel()

	// This is pretty easy because we have only one server at a time.
	// If the server goes away, we'll just drop the message on the floor.
	for {
		select {
		case m := <-sq:
			if m == nil {
				sq = x.sock.SendChannel()
				continue
			}
			if ep.ep.SendMsg(m) != nil {
				m.Free()
				return
			}
		case <-ep.cq:
			return
		case <-cq:
			return
		}
	}
}

func (x *client) receiver(ep *serverEp) {

	rq := x.sock.RecvChannel()
	cq := x.sock.CloseChannel()

	for {
		m := ep.ep.RecvMsg()
		if m == nil {
			return
		}

		select {
		case rq <- m:
		case <-cq:
			return
		}
	}
}

func (x *client) AddEndpoint(ep mangos.Endpoint) {
	server := &serverEp{cq: make(chan struct{}), ep: ep}
	x.Lock()
	if x.server != nil {
		// We already have a connection, reject this one.
		x.Unlock()
		ep.Close()
		return
	}
	x.server = server
	x.Unlock()

	x.w.Add()
	go x.receiver(server)
	go x.sender(server)
}

func (x *client) RemoveEndpoint(ep mangos.Endpoint) {
	x.Lock()
	if server := x.server; server != nil && server.ep == ep {
		x.server = nil
		close(server.cq)
	}
	x.Unlock()
}

func (*client) Number() uint16 {
	return mangos.ProtoClient
}

func (*client) Name() string {
	return "client"
}

func (*client) PeerNumber() uint16 {
	return mangos.ProtoServer
}

func (*client) PeerName() string {
	return "server"
}

func (x *client) SetOption(name string, v interface{}) error {
	var ok bool
	switch name {
	case mangos.OptionRaw:
		if x.raw, ok = v.(bool); !ok {
			return mangos.ErrBadValue
		}
		return nil
	default:
		return mangos.ErrBadOption
	}
}

func (x *client) GetOption(name string) (interface{}, error) {
	switch name {
	case mangos.OptionRaw:
		return x.raw, nil
	default:
		return nil, mangos.ErrBadOption
	}
}

// NewSocket allocates a new Socket using the CLIENT protocol.
func NewSocket() (mangos.Socket, error) {
	return mangos.MakeSocket(&client{}), nil
}
