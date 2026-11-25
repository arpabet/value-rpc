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
	s              *quic.Stream
	br             *bufio.Reader
	rid            int64
	reqType        valuerpc.MessageType
	registered     bool
	originatedHere bool // we opened this stream (we are the request initiator)
	writeClosed    bool // our write half has been FIN'd
	localDone      bool // == writeClosed; kept for the both-done check
	remoteDone     bool // peer FIN'd / reset (reader hit EOF)
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

	// readers counts the goroutines that may still hand a message to
	// ReadMessage: the accept loop and the stream readers (guarded by mu).
	// ended is closed when the QUIC connection is gone and the last of them has
	// returned, which is when ReadMessage has nothing left to wait for.
	readers   int
	ended     chan struct{}
	endedOnce sync.Once
}

func newQUICMsgConn(conn *quic.Conn, isClient bool, writeTimeout time.Duration) *quicMsgConn {
	t := &quicMsgConn{
		conn:     conn,
		isClient: isClient,
		writeTO:  writeTimeout,
		incoming: make(chan value.Map),
		done:     make(chan struct{}),
		streams:  make(map[int64]*quicStreamState),
		ended:    make(chan struct{}),
	}
	// Both sides accept peer-opened streams: the server for the client's
	// requests, the client for the server's reverse (server-initiated) requests
	// — vRPC is peer-symmetric, so either end can open a stream toward the other.
	t.readerStarts()
	if isClient {
		go t.clientAcceptLoop()
	} else {
		go t.serverAcceptLoop()
	}
	return t
}

// serverAcceptLoop accepts client-opened streams. The first stream carries the
// handshake; its first frame is read synchronously and funneled before any other
// stream is accepted, so the server's handshake() observes it first.
func (t *quicMsgConn) serverAcceptLoop() {
	defer t.readerReturns()

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
		t.readerStarts()
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
		t.readerStarts()
		go t.streamReader(st)
	}
}

// clientAcceptLoop accepts streams the server opens for server-initiated
// (reverse) requests. Unlike the server loop it has no handshake-first special
// case: the client opens the handshake stream itself.
func (t *quicMsgConn) clientAcceptLoop() {
	defer t.readerReturns()
	for {
		s, err := t.conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		st := &quicStreamState{s: s, br: bufio.NewReader(s)}
		t.readerStarts()
		go t.streamReader(st)
	}
}

// readerStarts counts one more goroutine that may hand a message to
// ReadMessage. Call it before starting the goroutine, which defers
// readerReturns.
func (t *quicMsgConn) readerStarts() {
	t.wg.Add(1)
	t.mu.Lock()
	t.readers++
	t.mu.Unlock()
}

// readerReturns is deferred by the accept loop and by every stream reader.
//
// The accept loop lasts as long as the QUIC connection, so the count reaches
// zero only once the connection is gone (the peer closed it, it timed out, the
// network failed) or Close was called: every stream has failed its reader by
// then, and a reader that still held a message has handed it over. Nothing
// would tell ReadMessage, which waits on the funnel and not on the connection:
// ended is what does. It is closed after the last message, not at the moment
// the connection goes, so that what arrived before the end is not lost to it:
// the refusal a server sends before it closes, for one.
func (t *quicMsgConn) readerReturns() {
	defer t.wg.Done()
	t.mu.Lock()
	t.readers--
	last := t.readers == 0
	t.mu.Unlock()
	if last {
		// The readers fail a moment before the connection reports itself gone.
		<-t.conn.Context().Done()
		t.endedOnce.Do(func() { close(t.ended) })
	}
}

func (t *quicMsgConn) streamReader(st *quicStreamState) {
	defer t.readerReturns()
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
		// FIN our write half once reading this frame means we will send nothing
		// more on the stream: the request initiator after the terminal response
		// (unary/get/handshake), or the put-stream consumer after the producer's
		// StreamEnd (it keeps sending StreamCredit until then). Based on who
		// opened the stream, so it holds for reverse (server-initiated) streams
		// too.
		if finOnRead(msgType(m), st.reqType, st.originatedHere) {
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
	case <-t.ended:
		// The QUIC connection is gone and nothing is left to hand over. Without
		// this the caller would wait for good, or until its read deadline: a
		// client would not learn that its server went away before it next wrote.
		select {
		case <-t.done:
			return nil, io.EOF // closed here
		default:
		}
		return nil, t.endErr()
	case <-timeout:
		return nil, os.ErrDeadlineExceeded
	}
}

// endErr says why the QUIC connection ended, as ReadMessage reports it: io.EOF
// when the peer closed it the way Close does, as a stream transport reports
// the end of its stream, and the reason otherwise (an idle timeout, a refused
// certificate, a peer that closed with an error).
func (t *quicMsgConn) endErr() error {
	err := context.Cause(t.conn.Context())
	if err == nil {
		return io.EOF
	}
	var app *quic.ApplicationError
	if xerrors.As(err, &app) && app.ErrorCode == 0 && app.ErrorMessage == "" {
		return io.EOF
	}
	return xerrors.Errorf("quic: connection ended: %w", err)
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
		if err == errLateFrame {
			return nil
		}
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
		// If the stream was FIN'd concurrently (by the reader on the terminal
		// response), the failure is not fatal; otherwise the connection is bad.
		t.mu.Lock()
		raced := st.writeClosed
		t.mu.Unlock()
		if raced {
			return nil
		}
		return err
	}

	if finOnWrite(st.originatedHere, mt, reqType) {
		t.finWrite(st)
	}
	return nil
}

// finOnWrite reports whether mt is the last message the sender will *send* on the
// stream (so its write half can be FIN'd), keyed by whether the sender initiated
// the request (originatedHere) rather than by client/server role — so it holds
// for reverse (server-initiated) streams too.
//
// The initiator's request is NOT terminal: a get-stream initiator still sends
// StreamCredit and may send CancelRequest, so it FINs only on its own StreamEnd
// (put) or CancelRequest — otherwise it FINs on reading the terminal
// response (finOnRead). The responder FINs after its terminal response; note a
// put-stream consumer (responder) keeps sending StreamCredit after StreamReady,
// so it does NOT FIN on StreamReady — it FINs on reading the producer's
// StreamEnd (finOnRead), matching the initiator's get-stream behavior.
//
// A chat is both at once for its initiator: it streams to the responder, and
// the responder streams to it. Its own StreamEnd ends only the first: the
// responder goes on streaming until it ends the chat, and has to be granted
// credit for what it sends. So the initiator of a chat does NOT FIN on its
// StreamEnd, or WriteMessage would swallow the StreamCredit it sends after it
// and the responder would stall for good once the credit it holds is spent. It
// FINs on reading the response that ends the chat (finOnRead), as the initiator
// of a get-stream does.
func finOnWrite(originatedHere bool, mt, reqType valuerpc.MessageType) bool {
	if originatedHere {
		if reqType == valuerpc.ChatRequest && mt == valuerpc.StreamEnd {
			return false
		}
		return mt == valuerpc.StreamEnd || mt == valuerpc.CancelRequest
	}
	switch mt {
	case valuerpc.HandshakeResponse, valuerpc.FunctionResponse, valuerpc.ErrorResponse, valuerpc.StreamEnd:
		return true
	}
	return false
}

// finOnRead reports whether receiving mt means the reader will send no more on
// the stream, so it can FIN its write half. The request initiator
// (originatedHere) FINs after the terminal response of a unary/get/handshake
// request, and of a chat: its responder ends a chat with its StreamEnd, whether
// or not the initiator had sent its own, or with the ErrorResponse it refuses
// or fails it with. The put-stream consumer (responder) FINs after the
// producer's StreamEnd (it kept sending StreamCredit until then).
func finOnRead(mt, reqType valuerpc.MessageType, originatedHere bool) bool {
	if originatedHere {
		switch reqType {
		case valuerpc.FunctionRequest:
			return mt == valuerpc.FunctionResponse || mt == valuerpc.ErrorResponse
		case valuerpc.GetStreamRequest, valuerpc.ChatRequest:
			return mt == valuerpc.StreamEnd || mt == valuerpc.ErrorResponse
		case valuerpc.HandshakeRequest:
			return mt == valuerpc.HandshakeResponse
		}
		return false
	}
	// Responder: only the put-stream consumer keeps sending (StreamCredit) after
	// StreamReady, so it FINs its write half when the producer ends the stream.
	if reqType == valuerpc.PutStreamRequest {
		return mt == valuerpc.StreamEnd || mt == valuerpc.ErrorResponse || mt == valuerpc.CancelRequest
	}
	return false
}

// opensRequest reports whether mt is the first message of a request, the one
// its initiator opens the request's stream with.
func opensRequest(mt valuerpc.MessageType) bool {
	switch mt {
	case valuerpc.HandshakeRequest, valuerpc.FunctionRequest, valuerpc.GetStreamRequest,
		valuerpc.PutStreamRequest, valuerpc.ChatRequest:
		return true
	}
	return false
}

// errLateFrame is what streamForWrite answers for a message of a request whose
// stream has been freed. WriteMessage drops such a message.
var errLateFrame = xerrors.New("quic: late frame for a request whose stream is freed")

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

	// No stream for this request id. A stream is opened by the message that
	// opens a request, and nothing else. Any other message for an id without a
	// stream is a late frame of a request whose stream has been freed: a
	// StreamCredit for values the consumer took after the stream had ended, a
	// StreamEnd or a CancelRequest that crossed the peer's terminal response.
	// Nobody waits for it and it has nowhere to go, so it is dropped, as a
	// write on a half that is FIN'd is (WriteMessage). Opening a stream for it
	// would leave that stream open for good at both ends, since no request
	// answers on it. Failing the write would cost more than the frame: a client
	// takes a failed write for a lost connection, and the sender of a server
	// gives up the rest of the batch the frame is in, the messages of other
	// requests.
	if !opensRequest(mt) {
		return nil, 0, errLateFrame
	}
	// A request is opened by the side that owns its id: the client owns the
	// handshake (0) and its positive request ids; the server owns the negative
	// (server-initiated / reverse) ids.
	originator := (t.isClient && rid >= 0) || (!t.isClient && rid < 0)
	if !originator {
		return nil, 0, xerrors.Errorf("quic: request %d is not this side's to open", rid)
	}

	s, err := t.conn.OpenStreamSync(context.Background())
	if err != nil {
		return nil, 0, err
	}
	st := &quicStreamState{s: s, br: bufio.NewReader(s), rid: rid, reqType: mt, registered: true, originatedHere: true}
	t.mu.Lock()
	t.streams[rid] = st
	t.mu.Unlock()
	t.readerStarts()
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
