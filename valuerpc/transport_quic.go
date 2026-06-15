/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"go.arpabet.com/value"
)

// QUIC transport (seam-fit, per-request streams). Each vRPC request maps to its
// own QUIC stream on the wire — the client opens a stream per request, the
// server accepts one per request — so requests have independent QUIC flow
// control and ordering, plus TLS 1.3 and connection migration for free. To keep
// the existing single-MsgConn RPC layer unchanged, inbound frames from all
// streams are funneled into one ReadMessage queue; that funnel means
// application-level slow-consumer head-of-line blocking is reduced but not fully
// eliminated (see TRANSPORTS.md §9).
//
// QUIC mandates TLS: NewQUICServer needs a *tls.Config with a certificate, and
// the client verifies it (system roots by default).

var (
	// QUICDialTimeout bounds the QUIC opening handshake on the client.
	QUICDialTimeout = 30 * time.Second
	// QUICKeepAlive is the QUIC keep-alive ping period (0 disables).
	QUICKeepAlive = 15 * time.Second
	// QUICMaxStreams caps concurrent requests (streams) per connection.
	QUICMaxStreams = int64(1 << 16)

	quicALPN = []string{"vrpc"}
)

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams: QUICMaxStreams,
		MaxIdleTimeout:     30 * time.Second,
		KeepAlivePeriod:    QUICKeepAlive,
	}
}

func quicServerTLS(conf *tls.Config) *tls.Config {
	c := conf.Clone()
	if len(c.NextProtos) == 0 {
		c.NextProtos = quicALPN
	}
	return c
}

func quicClientTLS(conf *tls.Config, addr string) *tls.Config {
	if conf == nil {
		conf = &tls.Config{}
	}
	c := conf.Clone()
	if len(c.NextProtos) == 0 {
		c.NextProtos = quicALPN
	}
	if c.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			c.ServerName = host
		}
	}
	return c
}

// --- per-stream state ---

type quicStreamState struct {
	s          *quic.Stream
	br         *bufio.Reader
	rid        int64
	reqType    MessageType
	registered bool
	localDone  bool // we FIN'd our write side
	remoteDone bool // peer FIN'd / reset (reader hit EOF)
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
	if m, err := quicReadFrame(st0.br); err == nil {
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
		m, err := quicReadFrame(st.br)
		if err != nil {
			t.markRemoteDone(st)
			return
		}
		t.register(m, st)
		if !t.funnel(m) {
			return
		}
	}
}

// register binds a stream to its request id and request type on the first frame.
func (t *quicMsgConn) register(m value.Map, st *quicStreamState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st.registered {
		return
	}
	st.registered = true
	st.reqType = quicMsgType(m)
	if rid, ok := quicRid(m); ok {
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
		return ErrClientClosed
	default:
	}

	rid, ok := quicRid(m)
	if !ok {
		return errors.New("quic: outgoing message has no request id")
	}
	mt := quicMsgType(m)

	st, reqType, err := t.streamForWrite(rid, mt)
	if err != nil {
		return err
	}

	if t.writeTO > 0 {
		_ = st.s.SetWriteDeadline(time.Now().Add(t.writeTO))
	}
	if err := quicWriteFrame(st.s, m); err != nil {
		return err
	}

	if t.localTerminal(mt, reqType) {
		_ = st.s.Close() // FIN our write side
		t.markLocalDone(st)
	}
	return nil
}

// streamForWrite returns the stream carrying rid, opening a new one on the
// client for a new request. It also returns the request type (read under lock).
func (t *quicMsgConn) streamForWrite(rid int64, mt MessageType) (*quicStreamState, MessageType, error) {
	t.mu.Lock()
	if st, ok := t.streams[rid]; ok {
		reqType := st.reqType
		t.mu.Unlock()
		return st, reqType, nil
	}
	t.mu.Unlock()

	if !t.isClient {
		return nil, 0, fmt.Errorf("quic: no stream for response to request %d", rid)
	}

	// New client request: open a stream (not under the lock — OpenStreamSync may
	// block on the peer's stream limit).
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

// localTerminal reports whether mt is the last message this side sends on the
// stream, so the write half can be FIN'd.
func (t *quicMsgConn) localTerminal(mt, reqType MessageType) bool {
	if t.isClient {
		switch mt {
		case HandshakeRequest, FunctionRequest, GetStreamRequest, StreamEnd, CancelRequest:
			return true
		}
		return false
	}
	switch mt {
	case HandshakeResponse, FunctionResponse, ErrorResponse, StreamEnd:
		return true
	case StreamReady:
		// A put-stream server sends only StreamReady, then nothing.
		return reqType == PutStreamRequest
	}
	return false
}

func (t *quicMsgConn) markLocalDone(st *quicStreamState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st.localDone = true
	if st.remoteDone {
		delete(t.streams, st.rid)
	}
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
		close(t.done)
		_ = t.conn.CloseWithError(0, "")
	})
	return nil
}

// tlsState exposes the QUIC connection's TLS state so valuerpc.PeerCertificates
// works over QUIC (mutual-TLS authorization).
func (t *quicMsgConn) tlsState() (tls.ConnectionState, bool) {
	return t.conn.ConnectionState().TLS, true
}

// --- message helpers ---

func quicMsgType(m value.Map) MessageType {
	if n, ok := GetNumberField(m, MessageTypeField); ok {
		return MessageType(n.Long())
	}
	return MessageType(-1)
}

func quicRid(m value.Map) (int64, bool) {
	if n, ok := GetNumberField(m, RequestIdField); ok {
		return n.Long(), true
	}
	return 0, false
}

func quicReadFrame(r *bufio.Reader) (value.Map, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if MaxFrameSize > 0 && int64(n) > int64(MaxFrameSize) {
		return nil, fmt.Errorf("frame too large: %d bytes (max %d)", n, MaxFrameSize)
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	msg, err := value.Unpack(payload, true)
	if err != nil {
		return nil, fmt.Errorf("msgpack unpack, %v", err)
	}
	if msg.Kind() != value.MAP {
		return nil, errors.New("expected msgpack map")
	}
	return msg.(value.Map), nil
}

func quicWriteFrame(w io.Writer, m value.Map) error {
	payload, err := value.Pack(m)
	if err != nil {
		return fmt.Errorf("msgpack pack, %v", err)
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

// NewQUICListener listens for QUIC connections. config must carry a server
// certificate; set config.ClientAuth + config.ClientCAs for mutual TLS.
func NewQUICListener(addr string, config *tls.Config, writeTimeout time.Duration) (Listener, error) {
	if config == nil {
		return nil, fmt.Errorf("quic listener requires a non-nil *tls.Config with a server certificate")
	}
	ql, err := quic.ListenAddr(addr, quicServerTLS(config), quicConfig())
	if err != nil {
		return nil, err
	}
	return &quicListener{ql: ql, writeTO: writeTimeout}, nil
}

func (l *quicListener) Accept() (MsgConn, error) {
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

// NewQUICDialer dials a QUIC server. A nil config verifies against the system
// root CAs (server name derived from the address); supply a config for custom
// CAs, a client certificate (mTLS), or test options.
func NewQUICDialer(addr string, config *tls.Config, writeTimeout time.Duration) Dialer {
	return &quicDialer{addr: addr, config: config, writeTO: writeTimeout}
}

func (d *quicDialer) Dial() (MsgConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), QUICDialTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, d.addr, quicClientTLS(d.config, d.addr), quicConfig())
	if err != nil {
		return nil, err
	}
	return newQUICMsgConn(conn, true, d.writeTO), nil
}
