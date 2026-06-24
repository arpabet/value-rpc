/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

var OutgoingQueueCap = 4096

// MaxConcurrentRequests bounds how many request handlers may run concurrently
// for a single serving client (one logical client / connection). A request that
// would exceed the limit is rejected with an error response instead of spawning
// another handler goroutine, so a flood cannot exhaust memory/goroutines (DoS).
// Rejection is explicit (it never blocks the connection read loop, which would
// head-of-line block every other request). Set to 0 to disable the limit.
var MaxConcurrentRequests int64 = 4096

// MaxConcurrentStreams bounds how many streaming requests (get-stream,
// put-stream, chat) may be open at once for a single serving client. Unlike
// MaxConcurrentRequests — which gates short-lived handler executions — this
// caps long-lived streams, each of which holds goroutines and buffers for its
// lifetime. A stream request over the limit is rejected with an error response.
// Set to 0 (default) for no limit.
var MaxConcurrentStreams int64 = 0

type servingClient struct {
	clientId    int64
	resumeMu    sync.Mutex // serializes the verify-and-advance of resumeToken
	resumeToken string     // last accepted reverse-hash-chain link; the next reconnect must hash forward to it (guarded by resumeMu)
	principal   string     // authenticated identity; set once, binds reconnect resumption
	activeConn  atomic.Value
	functionMap *sync.Map
	cfg         *serverConfig // per-server config (caps, queue sizes); never mutated

	logger *zap.Logger

	ctx    context.Context    // session context; parent of every request context
	cancel context.CancelFunc // cancels all in-flight handlers when the session ends

	outgoingQueue chan value.Map
	done          chan struct{} // closed by Close(); never close outgoingQueue (BUG-3)

	requestMap     sync.Map
	requestCancels sync.Map     // reqId(int64) -> context.CancelFunc for in-flight unary calls
	inFlight       atomic.Int64 // concurrent request handlers (MaxConcurrentRequests)
	liveStreams    atomic.Int64 // open streaming requests (MaxConcurrentStreams)

	pendingCalls      sync.Map     // server-initiated reqId -> chan callResult (server->client reverse unary)
	pendingStreams    sync.Map     // server-initiated reqId -> *clientStream (server->client reverse streams)
	lastServerRequest atomic.Int64 // allocator for server-initiated (negative) request ids

	closeOnce sync.Once
}

func NewServingClient(parent context.Context, clientId int64, resumeToken, principal string, conn vrpc.MsgConn, functionMap *sync.Map, logger *zap.Logger, cfg *serverConfig) *servingClient {

	client := &servingClient{
		clientId:      clientId,
		resumeToken:   resumeToken,
		principal:     principal,
		functionMap:   functionMap,
		cfg:           cfg,
		outgoingQueue: make(chan value.Map, cfg.outgoingQueueCap),
		done:          make(chan struct{}),
		logger:        logger,
	}
	client.ctx, client.cancel = context.WithCancel(parent)
	client.activeConn.Store(conn)

	// Exactly one long-lived sender for the lifetime of the serving client; it
	// always writes to the current activeConn, so reconnects must not start
	// another one (BUG-8).
	go client.sender()

	return client
}

// authorizeResume verifies a presented reverse-hash-chain link against the last
// one this session accepted and advances the stored value on success, tolerating
// dropped reconnect handshakes (the server hashes the presented value forward up
// to DefaultResyncWindow steps — see VerifyHashStep). A link equal to the last
// accepted one is treated as an idempotent retry — a lost handshake *response*
// makes the client re-present the same value — and is accepted without advancing.
// Anything else (a bogus or replayed link) is rejected. Caller must already have
// confirmed the principal matches; this only proves session continuity.
func (t *servingClient) authorizeResume(presented string) error {
	t.resumeMu.Lock()
	defer t.resumeMu.Unlock()
	if _, ok := vrpc.VerifyHashStep(presented, t.resumeToken, vrpc.DefaultResyncWindow); ok {
		t.resumeToken = presented // advance the chain to the link just proven
		return nil
	}
	if vrpc.SameToken(presented, t.resumeToken) {
		return nil // idempotent retry of the current link; do not advance
	}
	return xerrors.Errorf("handshake rejected: resumption token mismatch for client %d", t.clientId)
}

func (t *servingClient) Close() {

	t.closeOnce.Do(func() {
		// Cancel every in-flight handler's context, then signal the sender and
		// any blocked producers to stop. We must NOT close(outgoingQueue):
		// producers (handlers, streamers) may still send and would panic on a
		// closed channel (BUG-3).
		t.cancel()
		close(t.done)

		// Unblock the connection read loop so it can exit.
		if c := t.activeConn.Load(); c != nil {
			c.(vrpc.MsgConn).Close()
		}

		t.requestMap.Range(func(key, value interface{}) bool {
			sr := value.(*servingRequest)
			sr.Close()
			return true
		})

		// Tear down server-initiated reverse streams so their streamers/consumers
		// unblock (pending reverse-unary calls already unblock via <-t.done).
		t.pendingStreams.Range(func(_, v interface{}) bool {
			v.(*clientStream).Close()
			return true
		})
	})

}

func (t *servingClient) replaceConn(newConn vrpc.MsgConn) {

	oldConn := t.activeConn.Load()
	if oldConn != nil {
		oldConn.(vrpc.MsgConn).Close()
	}

	t.activeConn.Store(newConn)
	// The single sender (started in NewServingClient) picks up the new conn via
	// activeConn; starting another sender here caused duplicates (BUG-8).
}

func FunctionResult(requestId value.Number, result value.Value) value.Map {
	resp := value.EmptyMap(true).
		Put(vrpc.DefaultDialect.MessageTypeField, vrpc.FunctionResponse.Long()).
		Put(vrpc.DefaultDialect.RequestIdField, requestId)
	if result != nil {
		return resp.Put(vrpc.DefaultDialect.ResultField, result)
	} else {
		return resp
	}
}

func StreamReady(requestId value.Number) value.Map {
	return value.EmptyMap(true).
		Put(vrpc.DefaultDialect.MessageTypeField, vrpc.StreamReady.Long()).
		Put(vrpc.DefaultDialect.RequestIdField, requestId)
}

func StreamValue(requestId value.Number, val value.Value) value.Map {
	return value.EmptyMap(true).
		Put(vrpc.DefaultDialect.MessageTypeField, vrpc.StreamValue.Long()).
		Put(vrpc.DefaultDialect.RequestIdField, requestId).
		Put(vrpc.DefaultDialect.ValueField, val)
}

func StreamEnd(requestId value.Number, val value.Value) value.Map {
	resp := value.EmptyMap(true).
		Put(vrpc.DefaultDialect.MessageTypeField, vrpc.StreamEnd.Long()).
		Put(vrpc.DefaultDialect.RequestIdField, requestId)
	if val != nil {
		return resp.Put(vrpc.DefaultDialect.ValueField, val)
	} else {
		return resp
	}
}

// FunctionError builds an ErrorResponse carrying a machine-readable code and a
// formatted message.
func FunctionError(requestId value.Number, code vrpc.Code, format string, args ...interface{}) value.Map {
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	return value.EmptyMap(true).
		Put(vrpc.DefaultDialect.MessageTypeField, vrpc.ErrorResponse.Long()).
		Put(vrpc.DefaultDialect.RequestIdField, requestId).
		Put(vrpc.DefaultDialect.CodeField, value.Long(int64(code))).
		Put(vrpc.DefaultDialect.ErrorField, value.Utf8(msg))
}

// handlerErrCode maps a user-handler error to a Code: the code of a
// *valuerpc.Error, the matching code for a context deadline/cancellation (which
// a handler typically returns as ctx.Err()), else Internal.
func handlerErrCode(err error) vrpc.Code {
	if code := vrpc.CodeOf(err); code != vrpc.CodeUnknown {
		return code
	}
	switch err {
	case context.DeadlineExceeded:
		return vrpc.CodeDeadlineExceeded
	case context.Canceled:
		return vrpc.CodeCanceled
	default:
		return vrpc.CodeInternal
	}
}

// handlerError builds an ErrorResponse from an error returned by a user handler,
// honouring its code when it is a *valuerpc.Error and defaulting to Internal. For
// a coded error its plain message is used (not its String()) so the code prefix
// is not duplicated once the client rebuilds a *valuerpc.Error from the wire.
func handlerError(requestId value.Number, where string, err error) value.Map {
	msg := err.Error()
	if e, ok := err.(*vrpc.Error); ok {
		msg = e.Message
	}
	return FunctionError(requestId, handlerErrCode(err), "%s: %s", where, msg)
}

// responseCode extracts the Code from a synchronous response map: the ErrorResponse
// code, or CodeOK for a successful FunctionResponse. nil (a stream established with
// no synchronous reply) reads as CodeOK.
func responseCode(resp value.Map) vrpc.Code {
	if resp == nil {
		return vrpc.CodeOK
	}
	if mt, ok := vrpc.GetNumberField(resp, vrpc.DefaultDialect.MessageTypeField); ok && vrpc.MessageType(mt.Long()) == vrpc.ErrorResponse {
		if c, ok := vrpc.GetNumberField(resp, vrpc.DefaultDialect.CodeField); ok {
			return vrpc.Code(c.Long())
		}
		return vrpc.CodeUnknown
	}
	return vrpc.CodeOK
}

func (t *servingClient) sender() {

	for {

		select {
		case <-t.done:
			t.logger.Info("stop serving client", zap.Int64("clientId", t.clientId))
			return
		case resp, ok := <-t.outgoingQueue:
			if !ok {
				return
			}

			conn := t.activeConn.Load()
			if conn == nil {
				t.logger.Error("sender no active connection")
				continue
			}

			if err := conn.(vrpc.MsgConn).WriteMessage(resp); err != nil {
				// BUG-9 fix: do not re-enqueue (a full queue would deadlock) and
				// do not stop the only sender; the connection is replaced on
				// reconnect, after which sends resume.
				t.logger.Error("sender write message", zap.Error(err))
			}
		}

	}
}

func (t *servingClient) send(resp value.Map) error {
	select {
	case t.outgoingQueue <- resp:
		return nil
	case <-t.done:
		return vrpc.ErrClientClosed
	}
}

// callResult carries the outcome of a server->client call back to CallFunction.
type callResult struct {
	value value.Value
	err   error
}

// Compile-time check: a serving client is a full Peer (it can call its client
// back and open streams toward it), so the server side is symmetric with the
// client side, which also implements valuerpc.Peer.
var _ vrpc.Peer = (*servingClient)(nil)

// asPeer returns the serving client as a Peer — the surface a handler uses to
// call back into the connected client. Naming the conversion keeps the intent
// explicit at the otherwise-implicit interface boundary (see PeerFromContext).
func (t *servingClient) asPeer() vrpc.Peer { return t }

// CallFunction invokes a function the client registered (server->client reverse
// RPC) and returns its result. The request id is drawn from the server-initiated
// (negative) id space so it never collides with the client's own request ids on
// this connection. The call is bound by ctx and by the connection lifetime.
func (t *servingClient) CallFunction(ctx context.Context, name string, args value.Value) (value.Value, error) {
	reqId := t.nextServerRequestId()

	resCh := make(chan callResult, 1)
	t.pendingCalls.Store(reqId, resCh)
	defer t.pendingCalls.Delete(reqId)

	if err := t.send(vrpc.NewFunctionRequest(reqId, name, args)); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.done:
		return nil, vrpc.ErrClientClosed
	case r := <-resCh:
		return r.value, r.err
	}
}

// completePendingCall delivers a client's reply to the CallFunction waiting on
// reqId. An unknown reqId (a late reply arriving after ctx cancellation or
// disconnect) is ignored.
func (t *servingClient) completePendingCall(reqId value.Number, msgType vrpc.MessageType, resp value.Map) {
	v, ok := t.pendingCalls.LoadAndDelete(reqId.Long())
	if !ok {
		return
	}
	resCh := v.(chan callResult)
	if msgType == vrpc.ErrorResponse {
		code := vrpc.CodeUnknown
		if c, ok := vrpc.GetNumberField(resp, vrpc.DefaultDialect.CodeField); ok {
			code = vrpc.Code(c.Long())
		}
		msg := ""
		if s, ok := vrpc.GetStringField(resp, vrpc.DefaultDialect.ErrorField); ok {
			msg = s.String()
		}
		resCh <- callResult{err: &vrpc.Error{Code: code, Message: msg}}
		return
	}
	// FunctionResponse: an absent/Null result is a void return -> nil.
	result := resp.Get(vrpc.DefaultDialect.ResultField)
	if result != nil && result.Kind() == value.NULL {
		result = nil
	}
	resCh <- callResult{value: result}
}

func (t *servingClient) findFunction(name string) (*function, bool) {
	if fn, ok := t.functionMap.Load(name); ok {
		return fn.(*function), true
	}
	return nil, false
}

func (t *servingClient) serveFunctionRequest(ft functionType, req value.Map) {
	// This runs in its own goroutine; a panicking user handler must not crash
	// the whole server process.
	defer func() {
		if r := recover(); r != nil {
			t.logger.Error("recovered in serveFunctionRequest", zap.Any("recover", r))
		}
		t.inFlight.Add(-1) // paired with the increment in serveNewRequest
	}()

	// Unary requests complete synchronously here, so bracket them with
	// RequestBegin/RequestEnd (code from the response). Streams complete on their
	// own lifecycle and are metered via newServingRequest/deleteRequest instead.
	if ft == singleFunction {
		name := ""
		if method, ok := vrpc.GetStringField(req, vrpc.DefaultDialect.FunctionNameField); ok {
			name = method.String()
		}
		t.cfg.metrics.RequestBegin(name)
		start := time.Now()
		resp := t.doServeFunctionRequest(ft, req)
		t.cfg.metrics.RequestEnd(name, responseCode(resp), time.Since(start))
		if resp != nil {
			t.send(resp)
		}
		return
	}

	resp := t.doServeFunctionRequest(ft, req)
	if resp != nil {
		t.send(resp)
	}
}

func (t *servingClient) doServeFunctionRequest(ft functionType, req value.Map) value.Map {

	reqId, ok := vrpc.GetNumberField(req, vrpc.DefaultDialect.RequestIdField)
	if !ok {
		// Without a request id the response cannot be routed; reply with id 0.
		reqId = value.Long(0)
	}

	name, ok := vrpc.GetStringField(req, vrpc.DefaultDialect.FunctionNameField)
	if !ok {
		return FunctionError(reqId, vrpc.CodeInvalidArgument, "function name field not found")
	}

	fn, ok := t.findFunction(name.String())
	if !ok {
		return FunctionError(reqId, vrpc.CodeNotFound, "function not found %s", name.String())
	}

	args := req.Get(vrpc.DefaultDialect.ArgumentsField)
	if !vrpc.Verify(args, fn.args) {
		return FunctionError(reqId, vrpc.CodeInvalidArgument, "function '%s' invalid args%s", name.String(), valDetail(args))
	}

	if fn.ft != ft {
		return FunctionError(reqId, vrpc.CodeInvalidArgument, "function wrong type %s, expected %d, actual %d", name.String(), fn.ft, ft)
	}

	// Cap concurrent open streams (get-stream, put-stream, chat), which hold
	// goroutines/buffers for their lifetime. Reject over the limit rather than
	// letting a peer open unbounded long-lived streams.
	if ft != singleFunction {
		if max := t.cfg.maxConcurrentStreams; max > 0 && t.liveStreams.Load() >= max {
			return FunctionError(reqId, vrpc.CodeResourceExhausted, "server busy: too many concurrent streams (max %d)", max)
		}
	}

	// Per-request context: derived from the session context (cancelled on
	// disconnect/shutdown) and, for unary calls, bounded by the client's SLA so
	// deadlines and cancellation propagate to handlers. Streams are long-lived
	// and are bounded by client cancellation, not the per-call SLA.
	reqCtx, cancel := t.newRequestContext(req, ft)

	// Surface incoming request metadata (trace context, baggage) on the handler's
	// context, then let an optional extractor turn it into a real propagated
	// context (e.g. an OTel span context). Children inherit cancel.
	if md := vrpc.DecodeMetadata(req); md != nil {
		reqCtx = vrpc.ContextWithMetadata(reqCtx, md)
		if t.cfg.metadataExtractor != nil {
			reqCtx = t.cfg.metadataExtractor(reqCtx, md)
		}
	}

	// Surface the authenticated principal (derived by the handshake
	// Authenticator, bound to this session) on the handler's context, so
	// handlers attribute/authorize work to the connection-bound identity rather
	// than trusting an identity field in the request payload.
	if t.principal != "" {
		reqCtx = vrpc.ContextWithPrincipal(reqCtx, t.principal)
	}

	// Expose this connection's Peer handle so a handler can call back into the
	// client (server->client reverse RPC) via PeerFromContext.
	reqCtx = contextWithPeer(reqCtx, t.asPeer())

	switch fn.ft {
	case singleFunction:
		// Register the cancel so a CancelRequest for this in-flight unary call
		// cancels its context; always clean up so the map cannot grow unbounded.
		t.requestCancels.Store(reqId.Long(), cancel)
		defer func() {
			cancel()
			t.requestCancels.Delete(reqId.Long())
		}()
		res, err := fn.singleFn(reqCtx, args)
		if err != nil {
			return handlerError(reqId, "function "+name.String(), err)
		}
		if !vrpc.Verify(res, fn.res) {
			return FunctionError(reqId, vrpc.CodeInternal, "function '%s' invalid results%s", name.String(), valDetail(res))
		}
		return FunctionResult(reqId, res)

	case outgoingStream:
		// Streams outlive this call: the cancel belongs to the serving request
		// and fires when the stream is torn down (closeRequest / cancel / SLA).
		sr := t.newServingRequest(ft, reqId, name.String(), cancel)
		outC, err := fn.outStream(reqCtx, args)
		if err != nil {
			sr.setCode(handlerErrCode(err))
			sr.closeRequest(t)
			return handlerError(reqId, "out stream "+name.String(), err)
		}
		go sr.outgoingStreamer(outC, t)
		return nil

	case incomingStream:
		sr := t.newServingRequest(ft, reqId, name.String(), cancel)
		err := fn.inStream(reqCtx, args, sr.inC)
		if err != nil {
			sr.setCode(handlerErrCode(err))
			sr.closeRequest(t)
			return handlerError(reqId, "in stream "+name.String(), err)
		}
		return StreamReady(reqId)

	case chat:
		sr := t.newServingRequest(ft, reqId, name.String(), cancel)
		outC, err := fn.chat(reqCtx, args, sr.inC)
		if err != nil {
			sr.setCode(handlerErrCode(err))
			sr.closeRequest(t)
			return handlerError(reqId, "chat "+name.String(), err)
		}
		go sr.outgoingStreamer(outC, t)
		return nil
	}

	cancel()
	return FunctionError(reqId, vrpc.CodeInternal, "unsupported function %s type", name.String())

}

// newRequestContext derives a handler context from the session context. For a
// unary call carrying a positive SLA (TimeoutField, ms) the context also carries
// that deadline, so a cooperating handler observes the client's timeout. Streams
// are long-lived, so the SLA is not turned into a deadline for them; they are
// bounded by client cancellation (CancelRequest) instead.
func (t *servingClient) newRequestContext(req value.Map, ft functionType) (context.Context, context.CancelFunc) {
	if ft == singleFunction {
		if sla, ok := vrpc.GetNumberField(req, vrpc.DefaultDialect.TimeoutField); ok && sla.Long() > 0 {
			return context.WithTimeout(t.ctx, time.Duration(sla.Long())*time.Millisecond)
		}
	}
	return context.WithCancel(t.ctx)
}

func (t *servingClient) newServingRequest(ft functionType, reqId value.Number, name string, cancel context.CancelFunc) *servingRequest {
	sr := NewServingRequest(ft, reqId)
	sr.cancel = cancel // set before publishing so the read loop never races on it
	sr.method = name
	sr.start = time.Now()
	if ft == incomingStream || ft == chat {
		sr.setupInbound(t.cfg.incomingQueueCap, t.cfg.maxPending, t)
	}
	// Stream enters flight here; RequestEnd fires once at deleteRequest (the single
	// idempotent retirement point used by every teardown path).
	t.cfg.metrics.RequestBegin(name)
	t.requestMap.Store(reqId.Long(), sr)
	t.liveStreams.Add(1)
	// Grant the initial inbound window only after the request is published, so a
	// value the client sends in response cannot arrive before registration (which
	// would mis-dispatch it as a new request and drop it).
	if ft == incomingStream || ft == chat {
		sr.grantInitialInbound(t)
	}
	return sr
}

func (t *servingClient) findServingRequest(reqId value.Number) (*servingRequest, bool) {

	requestCtx, ok := t.requestMap.Load(reqId.Long())
	if !ok {
		return nil, false
	}

	return requestCtx.(*servingRequest), true

}

func (t *servingClient) deleteRequest(requestId value.Number) {
	// LoadAndDelete so the stream counter is decremented exactly once even if a
	// teardown path calls deleteRequest more than once for the same request. This
	// is also the single retirement point for stream metrics (RequestEnd fires
	// exactly once, paired with RequestBegin in newServingRequest).
	if v, existed := t.requestMap.LoadAndDelete(requestId.Long()); existed {
		t.liveStreams.Add(-1)
		sr := v.(*servingRequest)
		t.cfg.metrics.RequestEnd(sr.method, vrpc.Code(sr.code.Load()), time.Since(sr.start))
	}
}

func (t *servingClient) processRequest(req value.Map) error {
	//t.logger.Info("processRequest", zap.Stringer("req", req))

	mt, ok := vrpc.GetNumberField(req, vrpc.DefaultDialect.MessageTypeField)
	if !ok {
		return xerrors.Errorf("empty message type%s", reqDetail(req))
	}
	msgType := vrpc.MessageType(mt.Long())

	reqId, ok := vrpc.GetNumberField(req, vrpc.DefaultDialect.RequestIdField)
	if !ok {
		return xerrors.Errorf("request id not found%s", reqDetail(req))
	}

	// Inbound frames for a server-initiated stream (reverse stream): the client
	// is the responder. Route StreamReady/Value/End/Credit/Error by request id to
	// the client stream. Server-initiated ids are negative, so they never collide
	// with the client's own (positive) serving requests below.
	if cs, ok := t.findPendingStream(reqId); ok {
		return cs.handleInbound(msgType, req, t)
	}

	// Inbound FunctionResponse/ErrorResponse = the client replying to a
	// server->client call we initiated (reverse unary). Complete that pending call
	// rather than treating it as a new inbound request.
	if msgType == vrpc.FunctionResponse || msgType == vrpc.ErrorResponse {
		t.completePendingCall(reqId, msgType, req)
		return nil
	}

	if sr, ok := t.findServingRequest(reqId); ok {
		return sr.serveRunningRequest(msgType, req, t)
	} else {
		if msgType == vrpc.CancelRequest {
			// Cancel an in-flight unary call (best-effort) by cancelling its
			// context. Streams are handled above via their serving request.
			if c, ok := t.requestCancels.Load(reqId.Long()); ok {
				c.(context.CancelFunc)()
			}
			return nil
		}
		return t.serveNewRequest(msgType, req)
	}

}

func (t *servingClient) serveNewRequest(msgType vrpc.MessageType, req value.Map) error {

	var ft functionType
	switch msgType {
	case vrpc.FunctionRequest:
		ft = singleFunction
	case vrpc.GetStreamRequest:
		ft = outgoingStream
	case vrpc.PutStreamRequest:
		ft = incomingStream
	case vrpc.ChatRequest:
		ft = chat
	default:
		return xerrors.Errorf("unknown message type for new request%s", reqDetail(req))
	}

	// Reserve a handler slot. Over the limit we reject this one request with an
	// error response and keep the connection (and all other requests) healthy,
	// rather than blocking the read loop or spawning an unbounded goroutine.
	n := t.inFlight.Add(1)
	if max := t.cfg.maxConcurrentRequests; max > 0 && n > max {
		t.inFlight.Add(-1)
		if reqId, ok := vrpc.GetNumberField(req, vrpc.DefaultDialect.RequestIdField); ok {
			t.send(FunctionError(reqId, vrpc.CodeResourceExhausted, "server busy: too many concurrent requests (max %d)", max))
		}
		return nil
	}

	go t.serveFunctionRequest(ft, req)
	return nil
}
