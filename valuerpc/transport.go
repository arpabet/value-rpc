/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
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
// seam. ctx bounds and cancels the dial (connection establishment / handshake);
// once Dial returns, the connection's lifetime is independent of ctx.
type Dialer interface {
	Dial(ctx context.Context) (MsgConn, error)
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
	maxFrameSize int
}

// NewStreamListener listens on a stream network ("tcp" or "unix") and returns a
// Listener whose accepted connections use the length-prefix framing. keepAlive
// enables TCP keepalive (ignored for non-TCP networks); writeTimeout bounds each
// message write; maxFrameSize bounds inbound frames (<=0 uses MaxFrameSize).
func NewStreamListener(network, address string, keepAlive, writeTimeout time.Duration, maxFrameSize int) (Listener, error) {
	lis, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &streamListener{lis: lis, keepAlive: keepAlive, writeTimeout: writeTimeout, maxFrameSize: maxFrameSize}, nil
}

func (l *streamListener) Accept() (MsgConn, error) {
	c, err := l.lis.Accept()
	if err != nil {
		return nil, err
	}
	enableKeepAlive(c, l.keepAlive)
	return NewMsgConn(c, l.writeTimeout, l.maxFrameSize), nil
}

func (l *streamListener) Addr() net.Addr { return l.lis.Addr() }

func (l *streamListener) Close() error { return l.lis.Close() }

type streamDialer struct {
	network      string
	address      string
	socks5       string
	keepAlive    time.Duration
	writeTimeout time.Duration
	maxFrameSize int
}

// NewStreamDialer returns a Dialer for a stream network ("tcp" or "unix"). A
// non-empty socks5 (TCP only) routes the dial through a SOCKS5 proxy.
// maxFrameSize bounds inbound frames (<=0 uses MaxFrameSize).
func NewStreamDialer(network, address, socks5 string, keepAlive, writeTimeout time.Duration, maxFrameSize int) Dialer {
	return &streamDialer{
		network:      network,
		address:      address,
		socks5:       socks5,
		keepAlive:    keepAlive,
		writeTimeout: writeTimeout,
		maxFrameSize: maxFrameSize,
	}
}

func (d *streamDialer) Dial(ctx context.Context) (MsgConn, error) {
	if d.socks5 != "" {
		p, err := proxy.SOCKS5(d.network, d.socks5, nil, proxy.Direct)
		if err != nil {
			return nil, err
		}
		var c net.Conn
		if cd, ok := p.(proxy.ContextDialer); ok {
			c, err = cd.DialContext(ctx, d.network, d.address)
		} else {
			c, err = p.Dial(d.network, d.address) // older proxy.Dialer: ctx best-effort
		}
		if err != nil {
			return nil, err
		}
		return NewMsgConn(c, d.writeTimeout, d.maxFrameSize), nil
	}
	var nd net.Dialer
	c, err := nd.DialContext(ctx, d.network, d.address)
	if err != nil {
		return nil, err
	}
	enableKeepAlive(c, d.keepAlive)
	return NewMsgConn(c, d.writeTimeout, d.maxFrameSize), nil
}

// enableKeepAlive turns on TCP keepalive for *net.TCPConn; it is a no-op for
// other connection types (e.g. Unix sockets), which need no keepalive. A
// *tls.Conn is unwrapped so keepalive reaches the underlying TCP socket.
func enableKeepAlive(conn net.Conn, period time.Duration) {
	if tc, ok := conn.(*tls.Conn); ok {
		conn = tc.NetConn()
	}
	if tcp, ok := conn.(*net.TCPConn); ok && period > 0 {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(period)
	}
}

// --- address scheme parsing & transport factories ---

// ParseAddress splits an address into a network and an address. A "scheme://"
// prefix selects the network ("tcp://host:port", "unix:///path"); a bare
// address ("host:port", ":port") defaults to "tcp" for backward compatibility.
func ParseAddress(address string) (network, addr string) {
	if i := strings.Index(address, "://"); i >= 0 {
		return address[:i], address[i+3:]
	}
	return "tcp", address
}

// NewListener builds a Listener from a (possibly schemed) address. "unix"
// addresses get stale-socket cleanup; unsupported networks return an error
// (WebSocket arrives in a later phase).
func NewListener(address string, keepAlive, writeTimeout time.Duration, maxFrameSize int) (Listener, error) {
	network, addr := ParseAddress(address)
	switch network {
	case "unix", "unixpacket":
		return NewUnixListener(addr, writeTimeout, maxFrameSize)
	case "tcp", "tcp4", "tcp6":
		return NewStreamListener(network, addr, keepAlive, writeTimeout, maxFrameSize)
	case "ws":
		host, path := splitWSPath(addr)
		return NewWebSocketListener(host, path, writeTimeout, maxFrameSize, keepAlive)
	case "wss":
		return nil, fmt.Errorf("wss:// server needs TLS; mount valueserver.NewWebSocketHandler on your own TLS http.Server")
	case "tls":
		return nil, fmt.Errorf("tls:// server needs a *tls.Config with a certificate; use valueserver.NewTLSServer")
	case "quic":
		return nil, fmt.Errorf("quic:// is provided by the separate module go.arpabet.com/value-rpc/quic (valuequic.NewServer)")
	case "mem":
		return NewMemListener(addr)
	default:
		return nil, fmt.Errorf("unsupported listen network %q in address %q", network, address)
	}
}

// NewDialer builds a Dialer from a (possibly schemed) address. An unsupported
// network yields a Dialer whose Dial returns the error, so callers that do not
// return an error from construction (e.g. NewClient) surface it at connect time.
func NewDialer(address, socks5 string, keepAlive, writeTimeout time.Duration, maxFrameSize int) Dialer {
	network, addr := ParseAddress(address)
	switch network {
	case "unix", "unixpacket":
		return NewStreamDialer(network, addr, "", 0, writeTimeout, maxFrameSize)
	case "tcp", "tcp4", "tcp6":
		return NewStreamDialer(network, addr, socks5, keepAlive, writeTimeout, maxFrameSize)
	case "ws", "wss":
		return newWSDialer(network+"://"+addr, writeTimeout, maxFrameSize, keepAlive)
	case "tls":
		// Default config: verify against the system roots, server name derived
		// from the address. Use NewTLSDialer / NewTLSClient for custom CAs, a
		// client certificate (mTLS), or test options.
		return NewTLSDialer(addr, nil, keepAlive, writeTimeout, maxFrameSize)
	case "quic":
		return errDialer{fmt.Errorf("quic:// is provided by the separate module go.arpabet.com/value-rpc/quic (valuequic.NewClient)")}
	case "mem":
		return NewMemDialer(addr)
	default:
		return errDialer{fmt.Errorf("unsupported dial network %q in address %q", network, address)}
	}
}

type errDialer struct{ err error }

func (d errDialer) Dial(context.Context) (MsgConn, error) { return nil, d.err }

// NewUnixListener listens on a Unix-domain socket, cleaning up a stale socket
// file left behind by a previous (crashed) process first. It refuses to remove
// a path that is not a socket, so it cannot clobber a regular file.
func NewUnixListener(path string, writeTimeout time.Duration, maxFrameSize int) (Listener, error) {
	if err := removeStaleSocket(path); err != nil {
		return nil, err
	}
	return NewStreamListener("unix", path, 0, writeTimeout, maxFrameSize)
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to remove non-socket file at %q", path)
	}
	return os.Remove(path)
}
