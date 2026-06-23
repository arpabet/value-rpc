/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Package valuequic provides a QUIC transport for value-rpc. It lives in its own
// module (go.arpabet.com/value-rpc/quic) so that the heavyweight
// github.com/quic-go/quic-go dependency is only pulled in by programs that
// actually use QUIC; the core value-rpc module stays free of it.
//
// Each vRPC request maps to its own QUIC stream on the wire — the client opens a
// stream per request, the server accepts one per request — so requests have
// independent QUIC flow control and ordering, plus TLS 1.3 and connection
// migration. To keep the core single-MsgConn RPC layer unchanged, inbound frames
// from all streams are funneled into one ReadMessage queue; that funnel means
// application-level slow-consumer head-of-line blocking is reduced but not fully
// eliminated (see TRANSPORTS.md §9).
//
// QUIC mandates TLS: NewServer needs a *tls.Config with a certificate; set
// ClientAuth + ClientCAs for mutual TLS and read the verified client certificate
// via valuerpc.PeerCertificates in a connect-authorizer.
package valuequic

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

var (
	// DialTimeout bounds the QUIC opening handshake on the client.
	DialTimeout = 30 * time.Second
	// KeepAlivePeriod is the QUIC keep-alive ping period (0 disables).
	KeepAlivePeriod = 15 * time.Second
	// MaxStreams caps concurrent requests (streams) per connection.
	MaxStreams = int64(1 << 16)

	alpn = []string{"vrpc"}
)

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams: MaxStreams,
		MaxIdleTimeout:     30 * time.Second,
		KeepAlivePeriod:    KeepAlivePeriod,
	}
}

func serverTLS(conf *tls.Config) *tls.Config {
	c := conf.Clone()
	if len(c.NextProtos) == 0 {
		c.NextProtos = alpn
	}
	return c
}

func clientTLS(conf *tls.Config, addr string) *tls.Config {
	if conf == nil {
		conf = &tls.Config{}
	}
	c := conf.Clone()
	if len(c.NextProtos) == 0 {
		c.NextProtos = alpn
	}
	if c.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			c.ServerName = host
		}
	}
	return c
}

// --- public constructors ---

// NewServer creates a value-rpc server listening for QUIC connections. config
// must carry a server certificate; set config.ClientAuth + config.ClientCAs for
// mutual TLS.
func NewServer(addr string, config *tls.Config, logger *zap.Logger) (valueserver.Server, error) {
	lis, err := NewListener(addr, config, valueserver.DefaultTimeout)
	if err != nil {
		logger.Error("bind the quic server", zap.String("addr", addr), zap.Error(err))
		return nil, err
	}
	return valueserver.NewServerWithListener(lis, logger)
}

// NewClient creates a value-rpc client that dials a QUIC server. A nil config
// verifies against the system root CAs (server name derived from the address);
// supply a config for custom CAs, a client certificate (mTLS), or test options.
func NewClient(addr string, config *tls.Config) valueclient.Client {
	return valueclient.NewClientWithDialer(NewDialer(addr, config, valueclient.DefaultTimeout))
}

// NewListener listens for QUIC connections, returning a valuerpc.Listener.
func NewListener(addr string, config *tls.Config, writeTimeout time.Duration) (valuerpc.Listener, error) {
	if config == nil {
		return nil, xerrors.New("quic listener requires a non-nil *tls.Config with a server certificate")
	}
	ql, err := quic.ListenAddr(addr, serverTLS(config), quicConfig())
	if err != nil {
		return nil, err
	}
	return &quicListener{ql: ql, writeTO: writeTimeout}, nil
}

// NewDialer returns a valuerpc.Dialer for a QUIC server.
func NewDialer(addr string, config *tls.Config, writeTimeout time.Duration) valuerpc.Dialer {
	return &quicDialer{addr: addr, config: config, writeTO: writeTimeout}
}

// --- per-stream state ---

type quicStreamState struct {
	s           *quic.Stream
	br          *bufio.Reader
	rid         int64
	reqType     valuerpc.MessageType
	registered  bool
	writeClosed bool // our write half has been FIN'd
	localDone   bool // == writeClosed; kept for the both-done check
	remoteDone  bool // peer FIN'd / reset (reader hit EOF)
}

type quicMsgConn struct {
	conn     *quic.Conn
	isClient bool
	writeTO  time.Duration

	incoming  chan value.Map
	done      chan struct{}
	closeOnce sync.Once
	readDL    atomic.Pointer[time.Time]

	mu      sync.Mutex
	streams map[int64]*quicStreamState
	wg      sync.WaitGroup
}

func newQUICMsgConn(conn *quic.Conn, isClient bool, writeTimeout time.Duration) *quicMsgConn {
	t := &quicMsgConn{
		conn:     conn,
		isClient: isClient,
		writeTO:  writeTimeout,
		incoming: make(chan value.Map),
		done:     make(chan struct{}),
		streams:  make(map[int64]*quicStreamState),
	}
	if !isClient {
		t.wg.Add(1)
		go t.serverAcceptLoop()
	}
	return t
}

// serverAcceptLoop accepts client-opened streams. The first stream carries the
// handshake; its first frame is read synchronously and funneled before any other
// stream is accepted, so the server's handshake() observes it first.
func (t *quicMsgConn) serverAcceptLoop() {
	defer t.wg.Done()

	s0, err := t.conn.AcceptStream(context.Background())
	if err != nil {
		return
	}
	st0 := &quicStreamState{s: s0, br: bufio.NewReader(s0)}
	if m, err := readFrame(st0.br); err == nil {
		t.register(m, st0)
		if !t.funnel(m) {
			return
		}
		t.wg.Add(1)
		go t.streamReader(st0)
	} else {
		s0.CancelRead(0)
	}

	for {
		s, err := t.conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		st := &quicStreamState{s: s, br: bufio.NewReader(s)}
		t.wg.Add(1)
		go t.streamReader(st)
	}
}

func (t *quicMsgConn) streamReader(st *quicStreamState) {
	defer t.wg.Done()
	for {
		m, err := readFrame(st.br)
		if err != nil {
			t.markRemoteDone(st)
			return
		}
		t.register(m, st)
		if !t.funnel(m) {
			return
		}
		// The client has nothing more to send on a unary/get/handshake request
		// once the server's terminal response arrives; FIN the write half then
		// (not after sending the request — throttle/cancel may follow it).
		if t.isClient && finOnRead(msgType(m), st.reqType) {
			t.finWrite(st)
		}
	}
}

func (t *quicMsgConn) register(m value.Map, st *quicStreamState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st.registered {
		return
	}
	st.registered = true
	st.reqType = msgType(m)
	if rid, ok := ridOf(m); ok {
		st.rid = rid
		t.streams[rid] = st
	}
}

func (t *quicMsgConn) funnel(m value.Map) bool {
	select {
	case t.incoming <- m:
		return true
	case <-t.done:
		return false
	}
}

func (t *quicMsgConn) ReadMessage() (value.Map, error) {
	var timeout <-chan time.Time
	if dl := t.readDL.Load(); dl != nil && !dl.IsZero() {
		timer := time.NewTimer(time.Until(*dl))
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case m := <-t.incoming:
		return m, nil
	case <-t.done:
		return nil, io.EOF
	case <-timeout:
		return nil, os.ErrDeadlineExceeded
	}
}

func (t *quicMsgConn) WriteMessage(m value.Map) error {
	select {
	case <-t.done:
		return valuerpc.ErrClientClosed
	default:
	}

	rid, ok := ridOf(m)
	if !ok {
		return xerrors.New("quic: outgoing message has no request id")
	}
	mt := msgType(m)

	st, reqType, err := t.streamForWrite(rid, mt)
	if err != nil {
		return err
	}

	// The write half may already be FIN'd (e.g. a best-effort throttle arriving
	// after the request finished). Swallow it — it is not connection-fatal, and
	// propagating would make the core requestLoop reconnect the whole client.
	t.mu.Lock()
	closed := st.writeClosed
	t.mu.Unlock()
	if closed {
		return nil
	}

	if t.writeTO > 0 {
		_ = st.s.SetWriteDeadline(time.Now().Add(t.writeTO))
	}
	if err := writeFrame(st.s, m); err != nil {
		// If the stream was FIN'd concurrently (by the reader on the server's
		// terminal), the failure is not fatal; otherwise the connection is bad.
		t.mu.Lock()
		raced := st.writeClosed
		t.mu.Unlock()
		if raced {
			return nil
		}
		return err
	}

	if t.finOnWrite(mt, reqType) {
		t.finWrite(st)
	}
	return nil
}

// finOnWrite reports whether mt is the last message this side will *send* on the
// stream (so its write half can be FIN'd). The client's request is NOT terminal
// (throttle/cancel may follow); see finOnRead.
func (t *quicMsgConn) finOnWrite(mt, reqType valuerpc.MessageType) bool {
	if t.isClient {
		return mt == valuerpc.StreamEnd || mt == valuerpc.CancelRequest
	}
	switch mt {
	case valuerpc.HandshakeResponse, valuerpc.FunctionResponse, valuerpc.ErrorResponse, valuerpc.StreamEnd:
		return true
	case valuerpc.StreamReady:
		return reqType == valuerpc.PutStreamRequest // put-stream server sends nothing after StreamReady
	}
	return false
}

// finOnRead reports whether receiving mt means the client will send no more on
// this (request-only) stream, so the client can FIN its write half.
func finOnRead(mt, reqType valuerpc.MessageType) bool {
	switch reqType {
	case valuerpc.FunctionRequest:
		return mt == valuerpc.FunctionResponse || mt == valuerpc.ErrorResponse
	case valuerpc.GetStreamRequest:
		return mt == valuerpc.StreamEnd || mt == valuerpc.ErrorResponse
	case valuerpc.HandshakeRequest:
		return mt == valuerpc.HandshakeResponse
	}
	return false
}

// finWrite FINs (gracefully closes) the write half once, marking local done.
func (t *quicMsgConn) finWrite(st *quicStreamState) {
	t.mu.Lock()
	if st.writeClosed {
		t.mu.Unlock()
		return
	}
	st.writeClosed = true
	st.localDone = true
	bothDone := st.remoteDone
	if bothDone {
		delete(t.streams, st.rid)
	}
	t.mu.Unlock()
	_ = st.s.Close() // FIN: delivers buffered data, then signals end-of-write
}

func (t *quicMsgConn) streamForWrite(rid int64, mt valuerpc.MessageType) (*quicStreamState, valuerpc.MessageType, error) {
	t.mu.Lock()
	if st, ok := t.streams[rid]; ok {
		reqType := st.reqType
		t.mu.Unlock()
		return st, reqType, nil
	}
	t.mu.Unlock()

	if !t.isClient {
		return nil, 0, xerrors.Errorf("quic: no stream for response to request %d", rid)
	}

	s, err := t.conn.OpenStreamSync(context.Background())
	if err != nil {
		return nil, 0, err
	}
	st := &quicStreamState{s: s, br: bufio.NewReader(s), rid: rid, reqType: mt, registered: true}
	t.mu.Lock()
	t.streams[rid] = st
	t.mu.Unlock()
	t.wg.Add(1)
	go t.streamReader(st)
	return st, mt, nil
}

func (t *quicMsgConn) markRemoteDone(st *quicStreamState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st.remoteDone = true
	if st.localDone {
		delete(t.streams, st.rid)
	}
}

func (t *quicMsgConn) SetReadDeadline(deadline time.Time) error {
	t.readDL.Store(&deadline)
	return nil
}

func (t *quicMsgConn) RemoteAddr() string {
	if a := t.conn.RemoteAddr(); a != nil {
		return a.String()
	}
	return ""
}

func (t *quicMsgConn) Close() error {
	t.closeOnce.Do(func() {
		close(t.done)                    // unblock funneling readers
		_ = t.conn.CloseWithError(0, "") // error out blocked stream reads
		t.wg.Wait()                      // wait for accept loop + stream readers to exit
	})
	return nil
}

// TLSConnectionState exposes the QUIC connection's TLS state so
// valuerpc.PeerCertificates works over QUIC (mutual-TLS authorization).
func (t *quicMsgConn) TLSConnectionState() (tls.ConnectionState, bool) {
	return t.conn.ConnectionState().TLS, true
}

// --- message + framing helpers ---

func msgType(m value.Map) valuerpc.MessageType {
	if n, ok := valuerpc.GetNumberField(m, valuerpc.DefaultDialect.MessageTypeField); ok {
		return valuerpc.MessageType(n.Long())
	}
	return valuerpc.MessageType(-1)
}

func ridOf(m value.Map) (int64, bool) {
	if n, ok := valuerpc.GetNumberField(m, valuerpc.DefaultDialect.RequestIdField); ok {
		return n.Long(), true
	}
	return 0, false
}

func readFrame(r *bufio.Reader) (value.Map, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if valuerpc.MaxFrameSize > 0 && int64(n) > int64(valuerpc.MaxFrameSize) {
		return nil, xerrors.Errorf("frame too large: %d bytes (max %d)", n, valuerpc.MaxFrameSize)
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	m, err := value.Unpack(payload, true)
	if err != nil {
		return nil, xerrors.Errorf("msgpack unpack, %v", err)
	}
	if m.Kind() != value.MAP {
		return nil, xerrors.New("expected msgpack map")
	}
	return m.(value.Map), nil
}

func writeFrame(w io.Writer, m value.Map) error {
	payload, err := value.Pack(m)
	if err != nil {
		return xerrors.Errorf("msgpack pack, %v", err)
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	_, err = w.Write(frame)
	return err
}

// --- Listener / Dialer ---

type quicListener struct {
	ql      *quic.Listener
	writeTO time.Duration
}

func (l *quicListener) Accept() (valuerpc.MsgConn, error) {
	conn, err := l.ql.Accept(context.Background())
	if err != nil {
		return nil, err
	}
	return newQUICMsgConn(conn, false, l.writeTO), nil
}

func (l *quicListener) Addr() net.Addr { return l.ql.Addr() }

func (l *quicListener) Close() error { return l.ql.Close() }

type quicDialer struct {
	addr    string
	config  *tls.Config
	writeTO time.Duration
}

func (d *quicDialer) Dial(ctx context.Context) (valuerpc.MsgConn, error) {
	// Honour the caller's deadline; apply the package DialTimeout only when the
	// context carries none.
	if _, ok := ctx.Deadline(); !ok && DialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DialTimeout)
		defer cancel()
	}
	conn, err := quic.DialAddr(ctx, d.addr, clientTLS(d.config, d.addr), quicConfig())
	if err != nil {
		return nil, err
	}
	return newQUICMsgConn(conn, true, d.writeTO), nil
}
