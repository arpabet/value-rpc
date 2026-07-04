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
	"go.arpabet.com/value"
	"golang.org/x/xerrors"
)

// WebSocket transport. Unlike the stream transports, WebSocket is
// message-oriented: each vRPC message is one WebSocket *binary* frame carrying a
// MessagePack value.Map, so there is no length prefix (the frame is the
// boundary). MaxFrameSize maps to the WebSocket read limit.

// defaultWSDialTimeout bounds the WebSocket opening handshake only when a
// wsDialer is driven by a context that carries no deadline. On the normal client
// path valueclient.ConnectContext applies the per-instance valueclient.WithDialTimeout
// to the dial context, which supersedes this fallback. (The ping interval is the
// per-instance keepalive option, threaded in at construction.)
const defaultWSDialTimeout = 30 * time.Second

type wsMsgConn struct {
	conn       *websocket.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	remoteAddr string
	writeTO    time.Duration
	keepAlive  time.Duration
	codec      WireCodec // negotiated at handshake; msgpack (binary) or json (text)

	writeMu   sync.Mutex
	readDL    atomic.Pointer[time.Time]
	done      chan struct{}
	closeOnce sync.Once
}

// newWSMsgConn frames a WebSocket connection. maxFrameSize > 0 sets the read
// limit, <= 0 means unlimited; keepAlive <= 0 disables pings (a positive value is
// the ping interval). codec selects the wire format (nil defaults to msgpack).
// All are snapshotted here rather than read from globals.
func newWSMsgConn(conn *websocket.Conn, remoteAddr string, writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration, codec WireCodec) *wsMsgConn {
	if maxFrameSize > 0 {
		conn.SetReadLimit(int64(maxFrameSize))
	} else {
		conn.SetReadLimit(-1) // unlimited
	}
	if codec == nil {
		codec = MsgpackWireCodec
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &wsMsgConn{
		conn:       conn,
		ctx:        ctx,
		cancel:     cancel,
		remoteAddr: remoteAddr,
		writeTO:    writeTimeout,
		keepAlive:  keepAlive,
		codec:      codec,
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
	wantText := t.codec.TextFrames()
	if wantText && typ != websocket.MessageText {
		return nil, xerrors.Errorf("expected a text websocket frame for the json codec, got %v", typ)
	}
	if !wantText && typ != websocket.MessageBinary {
		// The most likely hand-rolled-client mistake: sending JSON text frames on
		// a msgpack connection because the codec was not negotiated. Make the
		// failure self-explaining (WEB.md §3.4).
		return nil, xerrors.Errorf("expected a binary websocket frame, got %v; JSON codec not negotiated (offer subprotocol %q)", typ, SubprotocolJson)
	}
	return t.codec.Decode(data)
}

func (t *wsMsgConn) WriteMessage(msg value.Map) error {
	payload, err := t.codec.Encode(msg)
	if err != nil {
		return xerrors.Errorf("%s encode, %v", t.codec.Name(), err)
	}
	frameType := websocket.MessageBinary
	if t.codec.TextFrames() {
		frameType = websocket.MessageText
	}
	ctx := t.ctx
	if t.writeTO > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(t.ctx, t.writeTO)
		defer cancel()
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.conn.Write(ctx, frameType, payload)
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

	codecs         []WireCodec               // enabled, server preference order; nil = {msgpack, json}
	originPatterns []string                  // AcceptOptions.OriginPatterns; nil = same-origin only
	compression    websocket.CompressionMode // permessage-deflate; default disabled
}

// wsOptions collects the WebSocket-specific listener settings that are not
// captured by the positional transport parameters. Build with the WSOption
// helpers.
type wsOptions struct {
	codecs         []WireCodec
	originPatterns []string
	compression    websocket.CompressionMode
}

// WSOption configures a WebSocket listener/handler beyond the positional
// transport parameters (codecs, CSRF origin policy, compression).
type WSOption func(*wsOptions)

// WithWSCodecs restricts the wire codecs the server will negotiate, in server
// preference order (e.g. only JsonWireCodec for a JSON-only public endpoint).
// The default (no option, or an empty list) enables both msgpack and json.
func WithWSCodecs(codecs ...WireCodec) WSOption {
	return func(o *wsOptions) { o.codecs = codecs }
}

// WithWSOriginPatterns sets the allowed browser Origins (coder/websocket
// AcceptOptions.OriginPatterns), e.g. "app.example.com". The default rejects
// cross-origin browser requests (Origin must match Host) — the safe CSRF
// default; set patterns to allow an api-subdomain split. "*" allows any origin.
func WithWSOriginPatterns(patterns ...string) WSOption {
	return func(o *wsOptions) { o.originPatterns = patterns }
}

// WithWSCompression enables permessage-deflate, which makes JSON frames roughly
// as compact as msgpack for typical payloads. Off by default.
func WithWSCompression() WSOption {
	return func(o *wsOptions) { o.compression = websocket.CompressionContextTakeover }
}

// subprotocols returns the offered subprotocol tokens in server preference
// order, derived from the enabled codecs (msgpack's empty token is omitted from
// the offer list but is still the default when a client offers nothing).
func (l *wsListener) subprotocols() []string {
	codecs := l.codecs
	if len(codecs) == 0 {
		codecs = []WireCodec{MsgpackWireCodec, JsonWireCodec}
	}
	var out []string
	for _, c := range codecs {
		if sp := c.Subprotocol(); sp != "" {
			out = append(out, sp)
		}
	}
	return out
}

// NewWebSocketListener creates a standalone WebSocket Listener that owns an HTTP
// server bound to addr and serves the vRPC upgrade endpoint at path
// (default "/"). maxFrameSize bounds inbound frames (<=0 uses MaxFrameSize);
// keepAlive is the ping interval (<=0 disables pings). Optional WSOptions set the
// enabled codecs, CSRF origin policy, and compression.
func NewWebSocketListener(addr, path string, writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration, opts ...WSOption) (Listener, error) {
	if path == "" {
		path = "/"
	}
	netLis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	var o wsOptions
	for _, opt := range opts {
		opt(&o)
	}
	l := &wsListener{
		addr:           netLis.Addr(),
		path:           path,
		writeTO:        writeTimeout,
		maxFrameSize:   maxFrameSize,
		keepAlive:      keepAlive,
		incoming:       make(chan MsgConn),
		done:           make(chan struct{}),
		codecs:         o.codecs,
		originPatterns: o.originPatterns,
		compression:    o.compression,
	}
	mux := http.NewServeMux()
	mux.Handle(path, l.Handler())
	l.httpSrv = &http.Server{Handler: mux}
	go l.httpSrv.Serve(netLis)
	return l, nil
}

// NewWebSocketHandler returns a Listener that does NOT own an HTTP server; mount
// its Handler() on your own http.ServeMux. This enables sharing a port with
// other HTTP routes and serving wss:// from your own TLS server. Optional
// WSOptions set the enabled codecs, CSRF origin policy, and compression.
func NewWebSocketHandler(writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration, opts ...WSOption) (Listener, http.Handler) {
	var o wsOptions
	for _, opt := range opts {
		opt(&o)
	}
	l := &wsListener{
		addr:           wsAddr("websocket"),
		writeTO:        writeTimeout,
		maxFrameSize:   maxFrameSize,
		keepAlive:      keepAlive,
		incoming:       make(chan MsgConn),
		done:           make(chan struct{}),
		codecs:         o.codecs,
		originPatterns: o.originPatterns,
		compression:    o.compression,
	}
	return l, l.Handler()
}

// Handler upgrades inbound HTTP requests to WebSocket and feeds them to Accept.
// The wire codec is negotiated by subprotocol (Sec-WebSocket-Protocol): a client
// offering "vrpc.json" gets JSON in text frames; offering nothing (every
// pre-negotiation client) gets msgpack in binary frames.
func (l *wsListener) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:    l.subprotocols(),
			OriginPatterns:  l.originPatterns,
			CompressionMode: l.compression,
		})
		if err != nil {
			return
		}
		codec := WireCodecFor(c.Subprotocol(), l.codecs)
		mc := newWSMsgConn(c, r.RemoteAddr, l.writeTO, l.maxFrameSize, l.keepAlive, codec)
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
		return nil, xerrors.New("websocket listener closed")
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
	codec        WireCodec // nil/msgpack offers no subprotocol; json offers "vrpc.json"
}

// newWSDialer builds a WebSocket dialer. keepAlive is the ping interval (<=0
// disables); maxFrameSize <=0 uses MaxFrameSize. The dial-timeout fallback is
// only used for a context without a deadline (see defaultWSDialTimeout).
func newWSDialer(url string, writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration) Dialer {
	return &wsDialer{
		url:          url,
		writeTO:      writeTimeout,
		maxFrameSize: maxFrameSize,
		keepAlive:    keepAlive,
		dialTimeout:  defaultWSDialTimeout,
	}
}

// NewWebSocketDialer builds a WebSocket dialer that opts into a specific wire
// codec. A msgpack (or nil) codec dials exactly like today (no subprotocol,
// binary frames). A json codec offers the "vrpc.json" subprotocol and requires
// the server to echo it — dialing a pre-negotiation server (which echoes none)
// fails with a clear error rather than sending text frames a binary-only server
// would reject.
func NewWebSocketDialer(url string, writeTimeout time.Duration, maxFrameSize int, keepAlive time.Duration, codec WireCodec) Dialer {
	return &wsDialer{
		url:          url,
		writeTO:      writeTimeout,
		maxFrameSize: maxFrameSize,
		keepAlive:    keepAlive,
		dialTimeout:  defaultWSDialTimeout,
		codec:        codec,
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
	codec := d.codec
	if codec == nil {
		codec = MsgpackWireCodec
	}
	var dialOpts *websocket.DialOptions
	if sp := codec.Subprotocol(); sp != "" {
		dialOpts = &websocket.DialOptions{Subprotocols: []string{sp}}
	}
	c, _, err := websocket.Dial(ctx, d.url, dialOpts)
	if err != nil {
		return nil, err
	}
	// When we offered a codec subprotocol, the server must have selected it;
	// otherwise it predates JSON negotiation and we must not send text frames.
	if sp := codec.Subprotocol(); sp != "" && c.Subprotocol() != sp {
		_ = c.Close(websocket.StatusProtocolError, "codec not negotiated")
		return nil, xerrors.Errorf("server did not negotiate wire codec %q (got subprotocol %q); it likely predates JSON support — use the msgpack codec", sp, c.Subprotocol())
	}
	return newWSMsgConn(c, d.url, d.writeTO, d.maxFrameSize, d.keepAlive, codec), nil
}

// splitWSPath splits a "host:port/path" address into host and path; a missing
// path defaults to "/".
func splitWSPath(addr string) (host, path string) {
	if i := strings.IndexByte(addr, '/'); i >= 0 {
		return addr[:i], addr[i:]
	}
	return addr, "/"
}
