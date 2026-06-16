/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
)

// In-memory transport: an in-process Listener/Dialer pair connected by Go
// channels. It carries value.Map messages BY REFERENCE — no MessagePack
// serialization and no sockets — so it is the fastest transport and is
// same-process only (safe because vRPC messages are immutable value.Maps).
//
// Use it for deterministic tests and to compose services in one binary now, then
// split them across a real socket later by changing only the address
// ("mem://billing" -> "tcp://billing:9000") with no other call-site changes.

type memAddr string

func (a memAddr) Network() string { return "mem" }
func (a memAddr) String() string  { return "mem://" + string(a) }

// process-wide registry of in-memory listeners, keyed by name.
var memRegistry = struct {
	mu sync.Mutex
	m  map[string]*memListener
}{m: make(map[string]*memListener)}

type memListener struct {
	name      string
	incoming  chan *memConn
	done      chan struct{}
	closeOnce sync.Once
}

// NewMemListener registers an in-process listener under name. A client dials it
// in the same process with NewMemDialer(name) or the address "mem://name".
func NewMemListener(name string) (Listener, error) {
	l := &memListener{
		name:     name,
		incoming: make(chan *memConn),
		done:     make(chan struct{}),
	}
	memRegistry.mu.Lock()
	defer memRegistry.mu.Unlock()
	if _, exists := memRegistry.m[name]; exists {
		return nil, fmt.Errorf("mem listener %q already registered", name)
	}
	memRegistry.m[name] = l
	return l, nil
}

func (l *memListener) Accept() (MsgConn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.done:
		return nil, fmt.Errorf("mem listener %q closed", l.name)
	}
}

func (l *memListener) Addr() net.Addr { return memAddr(l.name) }

func (l *memListener) Close() error {
	l.closeOnce.Do(func() {
		memRegistry.mu.Lock()
		if memRegistry.m[l.name] == l {
			delete(memRegistry.m, l.name)
		}
		memRegistry.mu.Unlock()
		close(l.done)
	})
	return nil
}

// NewMemDialer dials the in-process listener registered under name.
func NewMemDialer(name string) Dialer { return &memDialer{name: name} }

type memDialer struct{ name string }

func (d *memDialer) Dial() (MsgConn, error) {
	memRegistry.mu.Lock()
	l, ok := memRegistry.m[d.name]
	memRegistry.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no mem listener registered at %q", d.name)
	}
	client, server := newMemPipe(d.name)
	select {
	case l.incoming <- server:
		return client, nil
	case <-l.done:
		return nil, fmt.Errorf("mem listener %q closed", d.name)
	}
}

// memPipe is the shared close state of a bidirectional in-memory connection;
// closing either end tears down both directions (like net.Pipe).
type memPipe struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func (p *memPipe) close() { p.closeOnce.Do(func() { close(p.closed) }) }

type memConn struct {
	pipe   *memPipe
	recv   <-chan value.Map
	send   chan<- value.Map
	addr   string
	readDL atomic.Pointer[time.Time]
}

func newMemPipe(name string) (client, server *memConn) {
	p := &memPipe{closed: make(chan struct{})}
	c2s := make(chan value.Map) // client -> server
	s2c := make(chan value.Map) // server -> client
	client = &memConn{pipe: p, recv: s2c, send: c2s, addr: "mem://" + name}
	server = &memConn{pipe: p, recv: c2s, send: s2c, addr: "mem://" + name + "(peer)"}
	return client, server
}

func (c *memConn) ReadMessage() (value.Map, error) {
	var timeout <-chan time.Time
	if dl := c.readDL.Load(); dl != nil && !dl.IsZero() {
		timer := time.NewTimer(time.Until(*dl))
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case m := <-c.recv:
		return m, nil
	case <-c.pipe.closed:
		return nil, io.EOF
	case <-timeout:
		return nil, os.ErrDeadlineExceeded
	}
}

func (c *memConn) WriteMessage(m value.Map) error {
	select {
	case c.send <- m:
		return nil
	case <-c.pipe.closed:
		return ErrClientClosed
	}
}

func (c *memConn) SetReadDeadline(deadline time.Time) error {
	c.readDL.Store(&deadline)
	return nil
}

func (c *memConn) RemoteAddr() string { return c.addr }

func (c *memConn) Close() error {
	c.pipe.close()
	return nil
}
