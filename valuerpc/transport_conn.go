/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// "Bring your own connection" seam. These adapters turn an externally
// established byte stream into a value-rpc Dialer or Listener, so the RPC layer
// can run over a connection this package did not dial or accept itself.
//
// They are the integration point for transports kept outside core — obfuscation
// and pluggable transports, broker / rendezvous flows, WebRTC data channels — so
// none of those dependencies have to enter value-rpc. A transport produces an
// io.ReadWriteCloser, NewMsgConn frames it, and these adapters present it through
// the standard Dialer / Listener interfaces used by valueclient.NewClientWithDialer
// and valueserver.NewServerWithListener. The canonical obfuscation wiring is:
//
//	dialer := valuerpc.NewFuncDialer(func() (io.ReadWriteCloser, error) {
//		base, err := net.Dial("tcp", addr) // or any base transport
//		if err != nil {
//			return nil, err
//		}
//		return obfs.Client(base, policy), nil // wrap with an out-of-tree obfuscator
//	}, writeTimeout)
//	cli := valueclient.NewClientWithDialer(dialer)

// ErrConnConsumed is returned by a single-use Dialer once its connection has been
// handed out: a reconnect cannot re-establish a connection that was supplied
// directly. Use NewFuncDialer to (re)establish on demand.
var ErrConnConsumed = fmt.Errorf("value-rpc: connection already consumed")

// ErrListenerClosed is returned by Accept on a closed bring-your-own Listener.
var ErrListenerClosed = fmt.Errorf("value-rpc: listener closed")

// NewFuncDialer returns a Dialer that calls connect for every Dial — including
// reconnects after a link drops — and frames the returned stream with the
// length-prefix codec. It is the general client seam for custom and obfuscated
// transports: connect establishes the byte stream and the RPC layer frames and
// uses it unchanged.
func NewFuncDialer(connect func() (io.ReadWriteCloser, error), writeTimeout time.Duration) Dialer {
	return &funcDialer{connect: connect, writeTimeout: writeTimeout}
}

type funcDialer struct {
	connect      func() (io.ReadWriteCloser, error)
	writeTimeout time.Duration
}

func (d *funcDialer) Dial() (MsgConn, error) {
	c, err := d.connect()
	if err != nil {
		return nil, err
	}
	return NewMsgConn(c, d.writeTimeout, MaxFrameSize), nil
}

// NewSingleConnDialer returns a Dialer that frames conn and returns it on the
// first Dial, then returns ErrConnConsumed on any later Dial (e.g. an RPC-layer
// reconnect). Use it for a connection established out of band — a broker or
// rendezvous hand-off — when there is exactly one connection; use NewFuncDialer
// when a reconnect should re-establish the stream.
func NewSingleConnDialer(conn io.ReadWriteCloser, writeTimeout time.Duration) Dialer {
	return &singleConnDialer{conn: conn, writeTimeout: writeTimeout}
}

type singleConnDialer struct {
	mu           sync.Mutex
	conn         io.ReadWriteCloser
	writeTimeout time.Duration
	used         bool
}

func (d *singleConnDialer) Dial() (MsgConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.used {
		return nil, ErrConnConsumed
	}
	d.used = true
	return NewMsgConn(d.conn, d.writeTimeout, MaxFrameSize), nil
}

// NewAcceptListener returns a Listener whose Accept calls accept to obtain and
// frame the next connection. It is the general server seam for transports that
// produce connections out of band — a rendezvous broker, a WebRTC offer queue, an
// accept loop owned by another package, or an obfuscation layer wrapping a base
// listener. accept must block until a connection is available and return an error
// once the listener is closed. The optional stop hook is called once by Close (to
// unblock accept and release any underlying listener). addr is reported by Addr; a
// nil addr reports a placeholder so callers can still call Addr().String().
func NewAcceptListener(accept func() (io.ReadWriteCloser, error), addr net.Addr, stop func() error, writeTimeout time.Duration) Listener {
	return &funcListener{accept: accept, addr: addr, stop: stop, writeTimeout: writeTimeout}
}

type funcListener struct {
	accept       func() (io.ReadWriteCloser, error)
	addr         net.Addr
	stop         func() error
	writeTimeout time.Duration
	closeOnce    sync.Once
	closeErr     error
}

func (l *funcListener) Accept() (MsgConn, error) {
	c, err := l.accept()
	if err != nil {
		return nil, err
	}
	return NewMsgConn(c, l.writeTimeout, MaxFrameSize), nil
}

func (l *funcListener) Addr() net.Addr {
	if l.addr != nil {
		return l.addr
	}
	return byoAddr{}
}

func (l *funcListener) Close() error {
	l.closeOnce.Do(func() {
		if l.stop != nil {
			l.closeErr = l.stop()
		}
	})
	return l.closeErr
}

// NewSingleConnListener returns a Listener that serves the RPC protocol over a
// single externally established connection: Accept frames and yields conn once,
// then blocks until Close and returns ErrListenerClosed. For a broker that yields
// many connections over time, use NewAcceptListener. addr is reported by Addr; a
// nil addr reports a placeholder.
func NewSingleConnListener(conn io.ReadWriteCloser, addr net.Addr, writeTimeout time.Duration) Listener {
	ch := make(chan io.ReadWriteCloser, 1)
	ch <- conn
	done := make(chan struct{})
	accept := func() (io.ReadWriteCloser, error) {
		select {
		case c := <-ch:
			return c, nil
		case <-done:
			return nil, ErrListenerClosed
		}
	}
	var once sync.Once
	stop := func() error {
		once.Do(func() { close(done) })
		return nil
	}
	return NewAcceptListener(accept, addr, stop, writeTimeout)
}

// byoAddr is the placeholder net.Addr for bring-your-own listeners created without
// a meaningful address.
type byoAddr struct{}

func (byoAddr) Network() string { return "value-rpc" }
func (byoAddr) String() string  { return "" }
