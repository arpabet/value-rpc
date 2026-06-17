/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/pkg/errors"
	"go.arpabet.com/value"
)

// WebSocket transport. Unlike the stream transports, WebSocket is
// message-oriented: each vRPC message is one WebSocket *binary* frame carrying a
// MessagePack value.Map, so there is no length prefix (the frame is the
// boundary). MaxFrameSize maps to the WebSocket read limit.

// Deprecated: the WebSocket ping interval is now a per-instance option —
// valueserver.WithKeepAlivePeriod / valueclient.WithKeepAlivePeriod (threaded
// through NewWebSocketListener / NewDialer at construction). This global is no
// longer read by the library and is kept only for backward compatibility.
var WSKeepAlive = 15 * time.Second

// WSDialTimeout is the fallback bound on the WebSocket opening handshake, used
// only when a wsDialer is driven by a context that carries no deadline.
//
// Deprecated: prefer the per-instance dial bound valueclient.WithDialTimeout,
// which valueclient.ConnectContext applies to the dial context — that supersedes
// this fallback on the normal client path.
var WSDialTimeout = 30 * time.Second

type wsMsgConn struct {
	conn       *websocket.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	remoteAddr string
	writeTO    time.Duration
	keepAlive  time.Duration

	writeMu   sync.Mutex
	readDL    atomic.Pointer[time.Time]
	done      chan struct{}
	closeOnce sync.Once
}

// newWSMsgConn frames a WebSocket connection. maxFrameSize > 0 sets the read
// limit, <= 0 means unlimited; keepAlive <= 0 disables pings (a positive value is
// the ping interval). Both are snapshotted here rather than read from globals.
func newWSMsgConn(conn *websocket.Conn, remoteAddr string, writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration) *wsMsgConn {
	if maxFrameSize > 0 {
		conn.SetReadLimit(int64(maxFrameSize))
	} else {
		conn.SetReadLimit(-1) // unlimited
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &wsMsgConn{
		conn:       conn,
		ctx:        ctx,
		cancel:     cancel,
		remoteAddr: remoteAddr,
		writeTO:    writeTimeout,
		keepAlive:  keepAlive,
		done:       make(chan struct{}),
	}
	if keepAlive > 0 {
		go t.pinger()
	}
	return t
}

func (t *wsMsgConn) ReadMessage() (value.Map, error) {
	ctx := t.ctx
	if dl := t.readDL.Load(); dl != nil && !dl.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(t.ctx, *dl)
		defer cancel()
	}
	typ, data, err := t.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errors.Errorf("expected a binary websocket message, got %v", typ)
	}
	msg, err := value.Unpack(data, true)
	if err != nil {
		return nil, errors.Errorf("msgpack unpack, %v", err)
	}
	if msg.Kind() != value.MAP {
		return nil, errors.New("expected msgpack map")
	}
	return msg.(value.Map), nil
}

func (t *wsMsgConn) WriteMessage(msg value.Map) error {
	payload, err := value.Pack(msg)
	if err != nil {
		return errors.Errorf("msgpack pack, %v", err)
	}
	ctx := t.ctx
	if t.writeTO > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(t.ctx, t.writeTO)
		defer cancel()
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.conn.Write(ctx, websocket.MessageBinary, payload)
}

func (t *wsMsgConn) SetReadDeadline(deadline time.Time) error {
	t.readDL.Store(&deadline)
	return nil
}

func (t *wsMsgConn) RemoteAddr() string { return t.remoteAddr }

func (t *wsMsgConn) Close() error {
	t.closeOnce.Do(func() {
		close(t.done)
		t.cancel()
		_ = t.conn.Close(websocket.StatusNormalClosure, "")
	})
	return nil
}

// pinger sends periodic pings; a failed ping tears the connection down so the
// read loop unblocks and the peer is treated as dead.
func (t *wsMsgConn) pinger() {
	ticker := time.NewTicker(t.keepAlive)
	defer ticker.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(t.ctx, t.keepAlive)
			err := t.conn.Ping(ctx)
			cancel()
			if err != nil {
				t.Close()
				return
			}
		}
	}
}

// --- WebSocket listener ---

type wsAddr string

func (a wsAddr) Network() string { return "websocket" }
func (a wsAddr) String() string  { return string(a) }

type wsListener struct {
	addr         net.Addr
	path         string
	writeTO      time.Duration
	maxFrameSize int
	keepAlive    time.Duration
	incoming     chan MsgConn
	done         chan struct{}
	httpSrv      *http.Server // nil in handler (embedded) mode
	closeOnce    sync.Once
}

// NewWebSocketListener creates a standalone WebSocket Listener that owns an HTTP
// server bound to addr and serves the vRPC upgrade endpoint at path
// (default "/"). maxFrameSize bounds inbound frames (<=0 uses MaxFrameSize);
// keepAlive is the ping interval (<=0 disables pings).
func NewWebSocketListener(addr, path string, writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration) (Listener, error) {
	if path == "" {
		path = "/"
	}
	netLis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	l := &wsListener{
		addr:         netLis.Addr(),
		path:         path,
		writeTO:      writeTimeout,
		maxFrameSize: maxFrameSize,
		keepAlive:    keepAlive,
		incoming:     make(chan MsgConn),
		done:         make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.Handle(path, l.Handler())
	l.httpSrv = &http.Server{Handler: mux}
	go l.httpSrv.Serve(netLis)
	return l, nil
}

// NewWebSocketHandler returns a Listener that does NOT own an HTTP server; mount
// its Handler() on your own http.ServeMux. This enables sharing a port with
// other HTTP routes and serving wss:// from your own TLS server.
func NewWebSocketHandler(writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration) (Listener, http.Handler) {
	l := &wsListener{
		addr:         wsAddr("websocket"),
		writeTO:      writeTimeout,
		maxFrameSize: maxFrameSize,
		keepAlive:    keepAlive,
		incoming:     make(chan MsgConn),
		done:         make(chan struct{}),
	}
	return l, l.Handler()
}

// Handler upgrades inbound HTTP requests to WebSocket and feeds them to Accept.
func (l *wsListener) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mc := newWSMsgConn(c, r.RemoteAddr, l.writeTO, l.maxFrameSize, l.keepAlive)
		select {
		case l.incoming <- mc:
			// The HTTP handler must live for the whole connection: returning
			// here would tear the hijacked conn down. Block until it closes.
			select {
			case <-mc.done:
			case <-l.done:
				mc.Close()
			}
		case <-l.done:
			_ = c.Close(websocket.StatusGoingAway, "server shutting down")
		}
	})
}

func (l *wsListener) Accept() (MsgConn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.done:
		return nil, errors.New("websocket listener closed")
	}
}

func (l *wsListener) Addr() net.Addr { return l.addr }

func (l *wsListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
		if l.httpSrv != nil {
			_ = l.httpSrv.Close() // non-blocking; closes the listener and conns
		}
	})
	return nil
}

// --- WebSocket dialer ---

type wsDialer struct {
	url          string
	writeTO      time.Duration
	maxFrameSize int
	keepAlive    time.Duration
	dialTimeout  time.Duration
}

// newWSDialer captures the dial timeout from WSDialTimeout at construction.
// keepAlive is the ping interval (<=0 disables); maxFrameSize <=0 uses MaxFrameSize.
func newWSDialer(url string, writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration) Dialer {
	return &wsDialer{
		url:          url,
		writeTO:      writeTimeout,
		maxFrameSize: maxFrameSize,
		keepAlive:    keepAlive,
		dialTimeout:  WSDialTimeout,
	}
}

func (d *wsDialer) Dial(ctx context.Context) (MsgConn, error) {
	// Honour the caller's deadline; apply the dialer's own timeout only when the
	// context carries none (e.g. a direct caller without a bounded ConnectContext).
	if _, ok := ctx.Deadline(); !ok && d.dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.dialTimeout)
		defer cancel()
	}
	c, _, err := websocket.Dial(ctx, d.url, nil)
	if err != nil {
		return nil, err
	}
	return newWSMsgConn(c, d.url, d.writeTO, d.maxFrameSize, d.keepAlive), nil
}

// splitWSPath splits a "host:port/path" address into host and path; a missing
// path defaults to "/".
func splitWSPath(addr string) (host, path string) {
	if i := strings.IndexByte(addr, '/'); i >= 0 {
		return addr[:i], addr[i:]
	}
	return addr, "/"
}
