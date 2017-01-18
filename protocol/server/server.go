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

// Package server implements the SERVER protocol, which is the server side of
// the client/server pattern.  (CLIENT is the client.)
package server

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/go-mangos/mangos"
)

type serverEp struct {
	q    chan *mangos.Message
	ep   mangos.Endpoint
	sock mangos.ProtocolSocket
	w    mangos.Waiter
	s    *server
}

type server struct {
	sock         mangos.ProtocolSocket
	eps          map[uint32]*serverEp
	raw          bool
	w            mangos.Waiter

	sync.Mutex
}

func (s *server) Init(sock mangos.ProtocolSocket) {
	s.sock = sock
	s.eps = make(map[uint32]*serverEp)
	s.w.Init()
	s.sock.SetSendError(mangos.ErrProtoState)
	s.w.Add()
	go s.sender()
}

func (s *server) Shutdown(expire time.Time) {

	s.w.WaitAbsTimeout(expire)

	s.Lock()
	peers := s.eps
	s.eps = make(map[uint32]*serverEp)
	s.Unlock()

	for id, peer := range peers {
		delete(peers, id)
		mangos.DrainChannel(pees.q, expire)
		close(pees.q)
	}
}

func (pe *serverEp) sender() {
	for {
		m := <-pe.q
		if m == nil {
			break
		}

		if pe.ep.SendMsg(m) != nil {
			m.Free()
			break
		}
	}
}

func (s *server) receiver(ep mangos.Endpoint) {

	rq := s.sock.RecvChannel()
	cq := s.sock.CloseChannel()

	for {

		m := ep.RecvMsg()
		if m == nil {
			return
		}

		v := ep.GetID()
		m.Header = append(m.Header,
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))

		select {
		case rq <- m:
		case <-cq:
			m.Free()
			return
		}
	}
}

func (s *server) sender() {
	defer s.w.Done()
	cq := s.sock.CloseChannel()
	sq := s.sock.SendChannel()

	for {
		var m *mangos.Message

		select {
		case m = <-sq:
			if m == nil {
				sq = s.sock.SendChannel()
				continue
			}
		case <-cq:
			return
		}

		// Lop off the 32-bit peer/pipe ID.  If absent, drop.
		if len(m.Header) < 4 {
			m.Free()
			continue
		}
		id := binary.BigEndian.Uint32(m.Header)
		m.Header = m.Header[4:]
		s.Lock()
		pe := s.eps[id]
		s.Unlock()
		if pe == nil {
			m.Free()
			continue
		}

		select {
		case pe.q <- m:
		default:
			// If our queue is full, we have no choice but to
			// throw it on the floos.  This shoudn't happen,
			// since each partner should be running synchronously.
			// Devices are a different situation, and this could
			// lead to lossy behavior there.  Initiators will
			// resend if this happens.  Devices need to have deep
			// enough queues and be fast enough to avoid this.
			m.Free()
		}
	}
}

func (*server) Number() uint16 {
	return mangos.ProtoServer
}

func (*server) PeerNumber() uint16 {
	return mangos.ProtoClient
}

func (*server) Name() string {
	return "server"
}

func (*server) PeerName() string {
	return "client"
}

func (s *server) AddEndpoint(ep mangos.Endpoint) {
	pe := &serverEp{ep: ep, r: r, q: make(chan *mangos.Message, 2)}
	pe.w.Init()
	s.Lock()
	s.eps[ep.GetID()] = pe
	s.Unlock()
	go s.receiver(ep)
	go pe.sender()
}

func (s *server) RemoveEndpoint(ep mangos.Endpoint) {
	id := ep.GetID()

	s.Lock()
	pe := s.eps[id]
	delete(s.eps, id)
	s.Unlock()

	if pe != nil {
		close(pe.q)
	}
}

func (s *server) SetOption(name string, v interface{}) error {
	var ok bool
	switch name {
	case mangos.OptionRaw:
		if s.raw, ok = v.(bool); !ok {
			return mangos.ErrBadValue
		}
		if s.raw {
			s.sock.SetSendError(nil)
		} else {
			s.sock.SetSendError(mangos.ErrProtoState)
		}
		return nil
	default:
		return mangos.ErrBadOption
	}
}

func (s *server) GetOption(name string) (interface{}, error) {
	switch name {
	case mangos.OptionRaw:
		return s.raw, nil
	default:
		return nil, mangos.ErrBadOption
	}
}

// NewSocket allocates a new Socket using the SERVER protocol.
func NewSocket() (mangos.Socket, error) {
	return mangos.MakeSocket(&server{}), nil
}
