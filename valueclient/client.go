/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
)

type responseHandler func(resp value.Map)

var DefaultSendingCap = int64(1024)
var DefaultTimeoutMls = int64(1000) // one second

type rpcClient struct {
	dialer            valuerpc.Dialer
	clientId          int64
	sendingCap        int64
	conn              *syncConn
	lastRequest       atomic.Int64
	reconnects        atomic.Int64
	requestCtxMap     sync.Map
	connectionHandler atomic.Value
	errorHandler      atomic.Pointer[ErrorHandler]
	timeoutMls        atomic.Int64
	perfMonitor       atomic.Value
	shuttingDown      atomic.Bool
	reconnecting      atomic.Bool   // single-flights Reconnect against concurrent triggers
	closed            chan struct{} // closed by Close(); wakes the reconnect backoff sleep
	closeOnce         sync.Once
	sessionToken      atomic.Pointer[string]                  // server-issued; resent to authorize reconnect
	credential        atomic.Pointer[value.Value]             // client-supplied; validated by the server Authenticator
	maxPending        int                                     // per-stream receive pending bound
	logger            *zap.Logger                             // structured diagnostics; no-op unless WithLogger set
	metrics           valuerpc.Metrics                        // observability sink; no-op unless WithMetrics set
	metadata          func(context.Context) valuerpc.Metadata // per-request metadata injector; nil unless WithMetadata set
	reconnect         ReconnectPolicy                         // in-flight request disposition across a reconnect
	dialTimeout       time.Duration                           // bounds a dial when the context carries no deadline
	unaryInvoker      valuerpc.Invoker                        // invokeUnary wrapped by the configured unary interceptors

	functionMap sync.Map           // name -> *clientFunction the peer (server) may call/open (reverse RPC)
	servingMap  sync.Map           // server-initiated reqId -> *clientServingRequest (reverse streams)
	baseCtx     context.Context    // parent for inbound-call handler contexts; cancelled on Close
	baseCancel  context.CancelFunc // cancels baseCtx
}

func (t *rpcClient) loadSessionToken() string {
	if p := t.sessionToken.Load(); p != nil {
		return *p
	}
	return ""
}

func (t *rpcClient) SetCredential(credential value.Value) {
	t.credential.Store(&credential)
}

func (t *rpcClient) loadCredential() value.Value {
	if p := t.credential.Load(); p != nil {
		return *p
	}
	return nil
}

// NewClient creates a client for address. A bare "host:port" dials TCP; a scheme
// selects the transport: "tcp://host:port" or "unix:///path/to.sock". A non-empty
// socks5 (TCP only) routes through a SOCKS5 proxy. For full control use
// NewClientWithDialer.
func NewClient(address, socks5 string, opts ...ClientOption) Client {
	cfg := newClientConfig(opts)
	return NewClientWithDialer(valuerpc.NewDialer(address, socks5, cfg.keepAlive, cfg.writeTimeout, cfg.maxFrameSize), opts...)
}

// NewUnixClient creates a client that dials the Unix-domain socket at path.
func NewUnixClient(path string, opts ...ClientOption) Client {
	cfg := newClientConfig(opts)
	return NewClientWithDialer(valuerpc.NewStreamDialer("unix", path, "", 0, cfg.writeTimeout, cfg.maxFrameSize), opts...)
}

// NewWebSocketClient creates a client that dials a WebSocket URL, e.g.
// "ws://host:9000/rpc" or "wss://host/rpc".
func NewWebSocketClient(url string, opts ...ClientOption) Client {
	cfg := newClientConfig(opts)
	return NewClientWithDialer(valuerpc.NewDialer(url, "", cfg.keepAlive, cfg.writeTimeout, cfg.maxFrameSize), opts...)
}

// NewTLSClient creates a client that dials a TLS server over TCP. A nil config
// verifies against the system root CAs (server name derived from the address);
// supply a config for custom CAs, a client certificate (mTLS), or test options.
func NewTLSClient(address string, config *tls.Config, opts ...ClientOption) Client {
	cfg := newClientConfig(opts)
	return NewClientWithDialer(valuerpc.NewTLSDialer(address, config, cfg.keepAlive, cfg.writeTimeout, cfg.maxFrameSize), opts...)
}

// NewMemClient creates a client that connects to an in-process server registered
// under name (see valueserver.NewMemServer). Same-process only.
func NewMemClient(name string, opts ...ClientOption) Client {
	return NewClientWithDialer(valuerpc.NewMemDialer(name), opts...)
}

// NewClientWithDialer creates a client over any transport (TCP, Unix socket,
// WebSocket, …) supplied as a valuerpc.Dialer.
func NewClientWithDialer(dialer valuerpc.Dialer, opts ...ClientOption) Client {

	cfg := newClientConfig(opts)
	t := &rpcClient{
		dialer:      dialer,
		clientId:    rand.Int63(),
		sendingCap:  cfg.sendingCap,
		maxPending:  cfg.maxPending,
		logger:      cfg.logger,
		metrics:     cfg.metrics,
		metadata:    cfg.metadata,
		reconnect:   cfg.reconnect,
		dialTimeout: cfg.dialTimeout,
		conn:        NewSyncConn(),
		closed:      make(chan struct{}),
	}
	t.baseCtx, t.baseCancel = context.WithCancel(context.Background())

	t.timeoutMls.Store(cfg.timeoutMls)
	// Build the unary call path once: the actual call (invokeUnary) wrapped by the
	// configured interceptors (outermost first).
	t.unaryInvoker = valuerpc.ChainClientInterceptors(t.invokeUnary, cfg.interceptors...)
	return t
}

func (t *rpcClient) ClientId() int64 {
	return t.clientId
}

func (t *rpcClient) Stats() map[string]int64 {

	sendingLen, sendingCap := 0, 0
	if t.conn.hasConn() {
		sendingLen, sendingCap = t.conn.getConn().Stats()
	}

	return map[string]int64{
		"requests":   t.lastRequest.Load(),
		"reconnects": t.reconnects.Load(),
		"sendingLen": int64(sendingLen),
		"sendingCap": int64(sendingCap),
	}
}

func (t *rpcClient) Close() error {
	var self ErrorHandler = t
	t.errorHandler.Store(&self)
	t.shuttingDown.Store(true)
	t.closeOnce.Do(func() { close(t.closed) }) // wake any reconnect backoff
	if t.baseCancel != nil {
		t.baseCancel() // cancel in-flight inbound-call handlers
	}
	// Tear down streams this client is serving for the peer (reverse streams) so
	// their streamer/consumer goroutines unblock.
	t.servingMap.Range(func(_, v interface{}) bool {
		v.(*clientServingRequest).Close()
		return true
	})
	t.conn.reset()
	return nil
}

func (t *rpcClient) getConnectionHandler() ConnectionHandler {
	ch := t.connectionHandler.Load()
	if ch != nil {
		return ch.(ConnectionHandler)
	}
	return func(resp value.Map) {
		t.logger.Debug("connection established", zap.Stringer("handshake", resp))
	}
}

func (t *rpcClient) SetConnectionHandler(ch ConnectionHandler) {
	t.connectionHandler.Store(ch)
}

func (t *rpcClient) getErrorHandler() ErrorHandler {
	if p := t.errorHandler.Load(); p != nil {
		return *p
	}
	return t
}

func (t *rpcClient) SetErrorHandler(eh ErrorHandler) {
	t.errorHandler.Store(&eh)
}

func (t *rpcClient) SetMonitor(perfMonitor PerformanceMonitor) {
	t.perfMonitor.Store(perfMonitor)
}

func (t *rpcClient) SetTimeout(timeoutMls int64) {
	t.timeoutMls.Store(timeoutMls)
}

func (t *rpcClient) BadConnection(err error) {

	if t.shuttingDown.Load() {
		return
	}

	t.logger.Warn("bad connection, reconnecting", zap.Error(err))
	if err = t.Reconnect(); err != nil {
		t.logger.Error("reconnect failed", zap.Error(err))
		t.retryReconnect()
	}
}

// retryReconnect re-attempts the dial with exponential backoff after the initial
// reconnect failed. In-flight requests were already disposed by Reconnect; this
// only re-establishes the connection. No-op unless the reconnect policy enables
// retries (MaxAttempts != 0). The backoff sleep wakes immediately on Close.
func (t *rpcClient) retryReconnect() {
	p := t.reconnect
	if p.MaxAttempts == 0 {
		return
	}
	delay := p.InitialBackoff
	if delay <= 0 {
		delay = DefaultInitialBackoff
	}
	maxDelay := p.MaxBackoff
	if maxDelay <= 0 {
		maxDelay = DefaultMaxBackoff
	}

	for attempt := 1; p.MaxAttempts < 0 || attempt <= p.MaxAttempts; attempt++ {
		d := delay
		if p.Jitter { // equal jitter: half fixed, half random
			d = d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
		}
		select {
		case <-time.After(d):
		case <-t.closed:
			return
		}
		if t.shuttingDown.Load() {
			return
		}
		t.reconnects.Add(1)
		t.metrics.Reconnect()
		if err := t.Connect(); err == nil {
			t.logger.Info("reconnected after backoff", zap.Int("attempt", attempt))
			return
		} else {
			t.logger.Warn("reconnect attempt failed", zap.Int("attempt", attempt), zap.Error(err))
		}
		if delay = delay * 2; delay > maxDelay {
			delay = maxDelay
		}
	}
	t.logger.Error("giving up reconnect", zap.Int("maxAttempts", p.MaxAttempts))
}

func (t *rpcClient) ProtocolError(rest value.Map, err error) {
	t.logger.Error("protocol error: malformed message", zap.Error(err), zap.Stringer("message", rest))
}

func (t *rpcClient) StreamError(requestId int64, err error) {
	t.logger.Warn("stream error", zap.Int64("requestId", requestId), zap.Error(err))
}

func (t *rpcClient) IsActive() bool {
	return t.conn.hasConn()
}

func (t *rpcClient) Connect() error {
	return t.ConnectContext(context.Background())
}

// ConnectContext establishes the connection, bounding the dial by ctx. When ctx
// carries no deadline, the configured dial timeout (WithDialTimeout) is applied
// so a dial to an unreachable peer cannot block indefinitely. ctx only bounds the
// dial; the established connection's lifetime is independent of it.
func (t *rpcClient) ConnectContext(ctx context.Context) error {
	if t.conn.hasConn() {
		return nil
	}
	if _, ok := ctx.Deadline(); !ok && t.dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.dialTimeout)
		defer cancel()
	}
	return t.conn.connect(ctx, t.dialer, t.clientId, t.loadSessionToken(), t.loadCredential(), t.sendingCap, t.getResponseHandler(), t.getErrorHandler())
}

func (t *rpcClient) Reconnect() error {
	// Single-flight: collapse concurrent reconnect requests (e.g. several read/
	// write loops reporting the same drop, or a manual Reconnect racing one) into
	// one. A skipped caller trusts the in-flight attempt to re-establish the
	// connection; a genuine *later* failure (after this attempt completes) still
	// triggers a fresh reconnect. This prevents a reconnect storm where each new
	// connection's setup (the server closing the previous one on resumption) would
	// otherwise trigger yet another racing reconnect.
	if !t.reconnecting.CompareAndSwap(false, true) {
		return nil
	}
	defer t.reconnecting.Store(false)

	t.reconnects.Add(1)
	t.metrics.Reconnect()
	t.conn.reset()

	// Reconnect policy: requests in flight on the dropped connection can never
	// complete on it. Fail them fast by default; collect idempotent unary calls
	// for replay on the new connection. Only requests issued before the drop are
	// affected — ids issued concurrently (> cutoff) belong to the new connection.
	cutoff := t.lastRequest.Load()
	var replay []*rpcRequestCtx
	t.requestCtxMap.Range(func(_, v any) bool {
		rc := v.(*rpcRequestCtx)
		if rc.requestId > cutoff {
			return true
		}
		if rc.kind == unaryKind && t.reconnect.ReplayUnary != nil && t.reconnect.ReplayUnary(rc.Name()) {
			replay = append(replay, rc)
			return true // keep in the map; re-sent below
		}
		t.failRequest(rc, ErrConnectionLost)
		return true
	})

	if err := t.Connect(); err != nil {
		// Could not re-establish: fail the would-be-replayed requests too.
		for _, rc := range replay {
			t.failRequest(rc, ErrConnectionLost)
		}
		return err
	}

	// Re-send idempotent unary requests on the new connection. The request stays
	// continuously in flight (one RequestBegin/RequestEnd pair) and keeps its
	// original deadline budget (the SingleResp timer keeps running).
	for _, rc := range replay {
		t.logger.Debug("replaying idempotent unary across reconnect",
			zap.Int64("requestId", rc.requestId), zap.String("method", rc.Name()))
		t.conn.getConn().SendRequest(rc.req)
	}
	return nil
}

// failRequest applies the fail-fast outcome to an in-flight request: record the
// error, surface stream errors, close it (unblocking SingleResp / closing the
// stream channel and firing RequestEnd), and drop it from the request map.
func (t *rpcClient) failRequest(rc *rpcRequestCtx, err error) {
	rc.SetError(err)
	if rc.kind != unaryKind {
		t.getErrorHandler().StreamError(rc.requestId, err)
	}
	rc.Close()
	t.requestCtxMap.Delete(rc.requestId)
}

func (t *rpcClient) sendMetrics(requestCtx *rpcRequestCtx) {
	mon := t.perfMonitor.Load()
	if mon != nil {
		mon.(PerformanceMonitor)(requestCtx.Name(), requestCtx.Elapsed())
	}
}

func (t *rpcClient) processResponse(mt valuerpc.MessageType, resp value.Map, requestCtx *rpcRequestCtx) {

	switch mt {

	case valuerpc.FunctionResponse:
		// BUG-4/5 fix: an absent result field decodes as value.Null, not Go nil.
		// A void result must surface to the caller as nil, not a Null sentinel.
		result := resp.Get(valuerpc.DefaultDialect.ResultField)
		if result != nil && result.Kind() == value.NULL {
			result = nil
		}
		requestCtx.notifyResult(result)
		t.sendMetrics(requestCtx)
		requestCtx.Close()
		t.requestCtxMap.Delete(requestCtx.requestId)

	case valuerpc.ErrorResponse:
		// Surface the server's machine-readable code so callers can branch with
		// valuerpc.CodeOf / errors.As instead of string-matching.
		code := valuerpc.CodeUnknown
		if c, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.CodeField); ok {
			code = valuerpc.Code(c.Long())
		}
		serverErr := &valuerpc.Error{Code: code, Message: resp.GetString(valuerpc.DefaultDialect.ErrorField).String()}
		requestCtx.SetError(serverErr)
		t.getErrorHandler().StreamError(requestCtx.requestId, serverErr)
		requestCtx.Close()
		t.requestCtxMap.Delete(requestCtx.requestId)

	case valuerpc.StreamReady:
		requestCtx.notifyResult(nil)

	case valuerpc.StreamValue:
		// BUG-4 fix: an absent value field decodes as value.Null, not Go nil;
		// only deliver a real payload.
		if streamValue := resp.Get(valuerpc.DefaultDialect.ValueField); streamValue != nil && streamValue.Kind() != value.NULL {
			// BUG-6: notifyResult never blocks the response loop. Credit-based
			// flow control keeps a cooperating server within the buffer; if it
			// returns false the server ignored its credit and overran us — surface
			// it as a stream error (#13) and cancel once instead of silently
			// dropping into a closed buffer.
			if !requestCtx.notifyResult(streamValue) {
				requestCtx.cancelOnce.Do(func() {
					truncErr := valuerpc.NewError(valuerpc.CodeResourceExhausted,
						"stream %d truncated: server exceeded flow-control credit", requestCtx.requestId)
					requestCtx.SetError(truncErr)
					t.getErrorHandler().StreamError(requestCtx.requestId, truncErr)
					t.CancelRequest(requestCtx.requestId)
				})
			} else {
				t.metrics.StreamValue(requestCtx.Name())
			}
		}

	case valuerpc.StreamEnd:
		// BUG-4 fix: do not deliver a phantom value.Null at end of stream.
		if streamEndValue := resp.Get(valuerpc.DefaultDialect.ValueField); streamEndValue != nil && streamEndValue.Kind() != value.NULL {
			requestCtx.notifyResult(streamEndValue)
		}
		if requestCtx.TryGetClose() {
			t.requestCtxMap.Delete(requestCtx.requestId)
		}

	case valuerpc.CancelRequest:
		requestCtx.Close()
		t.requestCtxMap.Delete(requestCtx.requestId)

	case valuerpc.StreamCredit:
		// The server granted the client's streamOut more credit.
		if cr, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.CreditField); ok && requestCtx.sendCredit != nil {
			requestCtx.sendCredit.Grant(cr.Long())
		}

	default:
		t.getErrorHandler().ProtocolError(resp, ErrUnsupportedMessageType)

	}

}

// sendStreamCredit grants the server credit additional server->client stream
// values for requestId (credit-based flow control). Best-effort: it never blocks
// and is dropped if there is no connection.
func (t *rpcClient) sendStreamCredit(requestId int64, credit int64) {
	if !t.conn.hasConn() {
		return
	}
	t.conn.getConn().SendRequest(valuerpc.NewStreamCredit(value.Long(requestId), credit))
}

func (t *rpcClient) getResponseHandler() responseHandler {
	return func(resp value.Map) {

		// BUG-5 fix: GetNumber returns value.Zero (never nil) for a missing key,
		// so presence must be checked with GetNumberField, otherwise a message
		// with no type is silently treated as MessageType(0) = HandshakeResponse.
		mt, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.MessageTypeField)
		if !ok {
			t.getErrorHandler().ProtocolError(resp, ErrNoMessageType)
			return
		}
		msgType := valuerpc.MessageType(mt.Long())

		if msgType == valuerpc.HandshakeResponse {
			// Remember the server-issued session token so a later reconnect can
			// prove this is the same client and resume its session.
			if tok, ok := valuerpc.GetStringField(resp, valuerpc.DefaultDialect.SessionTokenField); ok {
				s := tok.String()
				t.sessionToken.Store(&s)
			}
			t.getConnectionHandler()(resp)
			return
		}

		// Inbound FunctionRequest = the peer (server) calling a unary function this
		// client registered (reverse RPC). Dispatch it to our functionMap rather
		// than correlating it to one of our own outstanding requests.
		if msgType == valuerpc.FunctionRequest {
			t.serveInboundCall(resp)
			return
		}

		// Inbound stream-open request = the server opening a stream this client
		// serves (reverse stream).
		if msgType == valuerpc.GetStreamRequest || msgType == valuerpc.PutStreamRequest || msgType == valuerpc.ChatRequest {
			t.serveInboundStream(msgType, resp)
			return
		}

		id, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.RequestIdField)
		if !ok {
			t.getErrorHandler().ProtocolError(resp, ErrIdFieldNotFound)
			return
		}

		// Running frame for a stream this client is serving (server-initiated,
		// negative id): route to its serving request before the client's own
		// (positive-id) request map.
		if entry, ok := t.servingMap.Load(id.Long()); ok {
			entry.(*clientServingRequest).serveRunning(msgType, resp, t)
			return
		}

		if entry, ok := t.requestCtxMap.Load(id.Long()); ok {
			requestCtx := entry.(*rpcRequestCtx)
			t.processResponse(msgType, resp, requestCtx)
		} else {
			t.getErrorHandler().ProtocolError(resp, ErrRequestNotFound)
		}
	}
}

// clientFnKind tags which handler shape a registered client function is.
type clientFnKind int

const (
	cfnUnary clientFnKind = iota
	cfnOut
	cfnIn
	cfnChat
)

// clientFunction is a handler the client registered for the peer (server) to
// call or open (reverse RPC); mirrors the server's per-function record.
type clientFunction struct {
	ft        clientFnKind
	args, res valuerpc.TypeDef
	fn        valuerpc.Function
	outStream valuerpc.OutgoingStream
	inStream  valuerpc.IncomingStream
	chat      valuerpc.Chat
}

// Compile-time check that the client implements the symmetric Peer surface.
var _ valuerpc.Peer = (*rpcClient)(nil)

// AddFunction registers a unary handler the peer (server) may invoke on this
// client via a server->client call. Safe to call before or after Connect;
// registrations persist across reconnects. Pass valuerpc.Any for args/res to
// accept any argument/result shape.
func (t *rpcClient) AddFunction(name string, args, res valuerpc.TypeDef, fn valuerpc.Function) error {
	if name == "" || fn == nil {
		return fmt.Errorf("valueclient: AddFunction requires a name and a handler")
	}
	t.functionMap.Store(name, &clientFunction{ft: cfnUnary, args: args, res: res, fn: fn})
	return nil
}

// serveInboundCall dispatches a server->client FunctionRequest to a registered
// handler and replies with the result or an error. The handler runs on its own
// goroutine so the single response loop is never blocked.
func (t *rpcClient) serveInboundCall(req value.Map) {
	reqId, ok := valuerpc.GetNumberField(req, valuerpc.DefaultDialect.RequestIdField)
	if !ok {
		t.getErrorHandler().ProtocolError(req, ErrIdFieldNotFound)
		return
	}
	name, ok := valuerpc.GetStringField(req, valuerpc.DefaultDialect.FunctionNameField)
	if !ok {
		t.reply(valuerpc.NewFunctionError(reqId, valuerpc.CodeInvalidArgument, "function name field not found"))
		return
	}
	entry, ok := t.functionMap.Load(name.String())
	if !ok {
		t.reply(valuerpc.NewFunctionError(reqId, valuerpc.CodeNotFound, "function not found %s", name.String()))
		return
	}
	cf := entry.(*clientFunction)
	if cf.ft != cfnUnary {
		t.reply(valuerpc.NewFunctionError(reqId, valuerpc.CodeInvalidArgument, "function '%s' is not unary", name.String()))
		return
	}
	args := req.Get(valuerpc.DefaultDialect.ArgumentsField)
	if !valuerpc.Verify(args, cf.args) {
		t.reply(valuerpc.NewFunctionError(reqId, valuerpc.CodeInvalidArgument, "function '%s' invalid args", name.String()))
		return
	}
	// TODO(reverse-rpc): bound inbound-call concurrency (cf. server MaxConcurrentRequests).
	go func() {
		ctx, cancel := context.WithCancel(t.baseCtx)
		defer cancel()
		res, err := cf.fn(ctx, args)
		if err != nil {
			t.reply(valuerpc.NewHandlerError(reqId, "function "+name.String(), err))
			return
		}
		if !valuerpc.Verify(res, cf.res) {
			t.reply(valuerpc.NewFunctionError(reqId, valuerpc.CodeInternal, "function '%s' invalid result", name.String()))
			return
		}
		t.reply(valuerpc.NewFunctionResult(reqId, res))
	}()
}

// reply best-effort sends a response to the peer over the current connection.
func (t *rpcClient) reply(resp value.Map) {
	if t.conn.hasConn() {
		t.conn.getConn().SendRequest(resp)
	}
}

func (t *rpcClient) newRequestCtx(requestId int64, kind streamKind, req value.Map, receiveCap int) *rpcRequestCtx {
	requestCtx := NewRequestCtx(requestId, kind, req, receiveCap, t.maxPending,
		func(n int64) { t.sendStreamCredit(requestId, n) })
	requestCtx.metrics = t.metrics // RequestEnd fires once at teardown (closeResult)
	t.metrics.RequestBegin(requestCtx.Name())
	t.requestCtxMap.Store(requestId, requestCtx)
	return requestCtx
}

func (t *rpcClient) ensureConnection(ctx context.Context) error {

	if !t.conn.hasConn() {
		return t.ConnectContext(ctx)
	}

	return nil
}

func (t *rpcClient) sendRequest(ctx context.Context, kind streamKind, req value.Map, receiveCap int) (*rpcRequestCtx, error) {

	err := t.ensureConnection(ctx)
	if err != nil {
		return nil, err
	}

	requestId := t.lastRequest.Add(1)
	req = req.Put(valuerpc.DefaultDialect.RequestIdField, value.Long(requestId))

	requestCtx := t.newRequestCtx(requestId, kind, req, receiveCap)

	t.conn.getConn().SendRequest(req)
	return requestCtx, nil

}

func (t *rpcClient) sendSystemRequest(requestId int64, mt valuerpc.MessageType) {

	// System messages (cancel, credit) are fire-and-forget on the existing
	// connection; the bounded default dial applies if a reconnect is needed.
	if err := t.ensureConnection(context.Background()); err != nil {
		return
	}

	req := value.EmptyMap(true).
		Put(valuerpc.DefaultDialect.MessageTypeField, mt.Long()).
		Put(valuerpc.DefaultDialect.RequestIdField, value.Long(requestId))

	t.conn.getConn().SendRequest(req)
}

func (t *rpcClient) CancelRequest(requestId int64) {
	t.sendSystemRequest(requestId, valuerpc.CancelRequest)
}

// effectiveTimeout is the timeout (ms) used for this call: the configured
// SetTimeout value, shortened to the context's remaining deadline when the
// context's deadline is sooner. It is sent to the server as the request SLA.
func (t *rpcClient) effectiveTimeout(ctx context.Context) int64 {
	base := t.timeoutMls.Load()
	if dl, ok := ctx.Deadline(); ok {
		remaining := time.Until(dl).Milliseconds()
		if remaining < 0 {
			remaining = 0
		}
		if base <= 0 || remaining < base {
			return remaining
		}
	}
	return base
}

// watchContext tears the request down if ctx is cancelled before the request
// finishes on its own. It is a no-op for a context that can never be cancelled
// (e.g. context.Background), and exits when the request completes, so it never
// leaks a goroutine.
func (t *rpcClient) watchContext(ctx context.Context, requestCtx *rpcRequestCtx) {
	if ctx.Done() == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			t.CancelRequest(requestCtx.requestId)
			requestCtx.Close()
		case <-requestCtx.done:
		}
	}()
}

// CallFunction makes a unary call, running it through any installed unary
// interceptors (WithInterceptors); the interceptor chain's innermost step is
// invokeUnary, the actual network call.
func (t *rpcClient) CallFunction(ctx context.Context, name string, args value.Value) (value.Value, error) {
	return t.unaryInvoker(ctx, name, args)
}

func (t *rpcClient) invokeUnary(ctx context.Context, name string, args value.Value) (value.Value, error) {

	timeout := t.effectiveTimeout(ctx)
	req := t.constructRequest(ctx, valuerpc.FunctionRequest, name, args, timeout)

	requestCtx, err := t.sendRequest(ctx, unaryKind, req, 1)
	if err != nil {
		return nil, err
	}

	res, err := requestCtx.SingleResp(ctx, t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.SetError(err) // record the outcome for RequestEnd metrics
		requestCtx.Close()
		return nil, err
	}

	return res, err
}

func (t *rpcClient) GetStream(ctx context.Context, name string, args value.Value, receiveCap int) (<-chan value.Value, int64, error) {

	timeout := t.effectiveTimeout(ctx)
	req := t.constructRequest(ctx, valuerpc.GetStreamRequest, name, args, timeout)

	requestCtx, err := t.sendRequest(ctx, getStreamKind, req, receiveCap)
	if err != nil {
		return nil, 0, err
	}

	_, err = requestCtx.SingleResp(ctx, t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.SetError(err) // record the outcome for RequestEnd metrics
		requestCtx.Close()
		return nil, 0, err
	}

	requestCtx.sendInitialCredit() // grant the server its initial send window
	t.watchContext(ctx, requestCtx)
	return requestCtx.MultiResp(), requestCtx.requestId, err
}

func (t *rpcClient) PutStream(ctx context.Context, name string, args value.Value, putCh <-chan value.Value) error {

	timeout := t.effectiveTimeout(ctx)
	req := t.constructRequest(ctx, valuerpc.PutStreamRequest, name, args, timeout)

	requestCtx, err := t.sendRequest(ctx, putStreamKind, req, 1)
	if err != nil {
		return err
	}

	_, err = requestCtx.SingleResp(ctx, t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.SetError(err) // record the outcome for RequestEnd metrics
		requestCtx.Close()
		return err
	}

	t.watchContext(ctx, requestCtx)
	go t.streamOut(requestCtx, putCh)

	return nil
}

func (t *rpcClient) Chat(ctx context.Context, name string, args value.Value, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error) {

	timeout := t.effectiveTimeout(ctx)
	req := t.constructRequest(ctx, valuerpc.ChatRequest, name, args, timeout)

	requestCtx, err := t.sendRequest(ctx, chatKind, req, receiveCap+1)
	if err != nil {
		return nil, 0, err
	}

	_, err = requestCtx.SingleResp(ctx, t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.SetError(err) // record the outcome for RequestEnd metrics
		requestCtx.Close()
		return nil, 0, err
	}

	requestCtx.sendInitialCredit() // grant the server its initial send window (chat output)
	t.watchContext(ctx, requestCtx)
	go t.streamOut(requestCtx, putCh)

	return requestCtx.MultiResp(), requestCtx.requestId, nil
}

func (t *rpcClient) streamOut(requestCtx *rpcRequestCtx, putCh <-chan value.Value) {

	for requestCtx.IsPutOpen() {

		val, ok := <-putCh
		if !ok {
			endReq := value.EmptyMap(true).
				Put(valuerpc.DefaultDialect.MessageTypeField, valuerpc.StreamEnd.Long()).
				Put(valuerpc.DefaultDialect.RequestIdField, value.Long(requestCtx.requestId))
			t.conn.getConn().SendRequest(endReq)
			break
		}

		// Credit-based flow control: wait for the server to have buffer space
		// before sending. Only this goroutine blocks. A closed gate (teardown)
		// ends the stream.
		if requestCtx.sendCredit != nil && !requestCtx.sendCredit.Acquire() {
			break
		}

		nextReq := value.EmptyMap(true).
			Put(valuerpc.DefaultDialect.MessageTypeField, valuerpc.StreamValue.Long()).
			Put(valuerpc.DefaultDialect.RequestIdField, value.Long(requestCtx.requestId)).
			Put(valuerpc.DefaultDialect.ValueField, val)

		t.conn.getConn().SendRequest(nextReq)
		t.metrics.StreamValue(requestCtx.Name())

	}

	if requestCtx.TryPutClose() {
		t.requestCtxMap.Delete(requestCtx.requestId)
	}

}

func (t *rpcClient) constructRequest(ctx context.Context, mt valuerpc.MessageType, name string, args value.Value, timeout int64) value.Map {

	req := value.EmptyMap(true).
		Put(valuerpc.DefaultDialect.MessageTypeField, mt.Long()).
		Put(valuerpc.DefaultDialect.FunctionNameField, value.Utf8(name)).
		Put(valuerpc.DefaultDialect.ArgumentsField, args)

	if timeout > 0 {
		req = req.Put(valuerpc.DefaultDialect.TimeoutField, value.Long(timeout))
	}

	// Inject request metadata (trace context, baggage) from the call's context.
	if t.metadata != nil {
		if md := valuerpc.EncodeMetadata(t.metadata(ctx)); md != nil {
			req = req.Put(valuerpc.DefaultDialect.MetadataField, md)
		}
	}

	return req
}
