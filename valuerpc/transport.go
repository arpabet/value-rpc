/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc

import (
	"net"
	"time"

	"golang.org/x/net/proxy"
)

// Listener accepts inbound connections and hands them back as MsgConns. It is
// the server-side transport seam: TCP, Unix sockets, and WebSocket each provide
// their own implementation, while the RPC layer only ever sees MsgConn.
type Listener interface {
	// Accept waits for and returns the next connection, already framed.
	Accept() (MsgConn, error)
	// Addr returns the address the listener is bound to.
	Addr() net.Addr
	// Close stops the listener; pending Accept calls return an error.
	Close() error
}

// Dialer establishes a single outbound MsgConn. It is the client-side transport
// seam.
type Dialer interface {
	Dial() (MsgConn, error)
}

// --- stream transport: length-prefix framed reliable byte streams ---
//
// Works for any stream network that net.Listen / net.Dial support — "tcp" and
// "unix" in particular. The wire framing is the 4-byte length prefix implemented
// by messageConnAdapter (see rpc.go), identical across stream networks.

type streamListener struct {
	lis          net.Listener
	keepAlive    time.Duration
	writeTimeout time.Duration
}

// NewStreamListener listens on a stream network ("tcp" or "unix") and returns a
// Listener whose accepted connections use the length-prefix framing. keepAlive
// enables TCP keepalive (ignored for non-TCP networks); writeTimeout bounds each
// message write.
func NewStreamListener(network, address string, keepAlive, writeTimeout time.Duration) (Listener, error) {
	lis, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &streamListener{lis: lis, keepAlive: keepAlive, writeTimeout: writeTimeout}, nil
}

func (l *streamListener) Accept() (MsgConn, error) {
	c, err := l.lis.Accept()
	if err != nil {
		return nil, err
	}
	enableKeepAlive(c, l.keepAlive)
	return NewMsgConn(c, l.writeTimeout), nil
}

func (l *streamListener) Addr() net.Addr { return l.lis.Addr() }

func (l *streamListener) Close() error { return l.lis.Close() }

type streamDialer struct {
	network      string
	address      string
	socks5       string
	keepAlive    time.Duration
	writeTimeout time.Duration
}

// NewStreamDialer returns a Dialer for a stream network ("tcp" or "unix"). A
// non-empty socks5 (TCP only) routes the dial through a SOCKS5 proxy.
func NewStreamDialer(network, address, socks5 string, keepAlive, writeTimeout time.Duration) Dialer {
	return &streamDialer{
		network:      network,
		address:      address,
		socks5:       socks5,
		keepAlive:    keepAlive,
		writeTimeout: writeTimeout,
	}
}

func (d *streamDialer) Dial() (MsgConn, error) {
	if d.socks5 != "" {
		p, err := proxy.SOCKS5(d.network, d.socks5, nil, proxy.Direct)
		if err != nil {
			return nil, err
		}
		c, err := p.Dial(d.network, d.address)
		if err != nil {
			return nil, err
		}
		return NewMsgConn(c, d.writeTimeout), nil
	}
	c, err := net.Dial(d.network, d.address)
	if err != nil {
		return nil, err
	}
	enableKeepAlive(c, d.keepAlive)
	return NewMsgConn(c, d.writeTimeout), nil
}

// enableKeepAlive turns on TCP keepalive for *net.TCPConn; it is a no-op for
// other connection types (e.g. Unix sockets), which need no keepalive.
func enableKeepAlive(conn net.Conn, period time.Duration) {
	if tcp, ok := conn.(*net.TCPConn); ok && period > 0 {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(period)
	}
}
