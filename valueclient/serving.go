/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// AddOutgoingStream registers a server-openable stream the client *produces*
// values on (the server reaches it with GetStream). Mirrors
// valueserver.AddOutgoingStream.
func (t *rpcClient) AddOutgoingStream(name string, args valuerpc.TypeDef, fn valuerpc.OutgoingStream) error {
	if name == "" || fn == nil {
		return fmt.Errorf("valueclient: AddOutgoingStream requires a name and a handler")
	}
	t.functionMap.Store(name, &clientFunction{ft: cfnOut, args: args, outStream: fn})
	return nil
}

// AddIncomingStream registers a server-openable stream the client *consumes*
// values from (the server reaches it with PutStream).
func (t *rpcClient) AddIncomingStream(name string, args valuerpc.TypeDef, fn valuerpc.IncomingStream) error {
	if name == "" || fn == nil {
		return fmt.Errorf("valueclient: AddIncomingStream requires a name and a handler")
	}
	t.functionMap.Store(name, &clientFunction{ft: cfnIn, args: args, inStream: fn})
	return nil
}

// AddChat registers a server-openable bidirectional stream (the server reaches
// it with Chat).
func (t *rpcClient) AddChat(name string, args valuerpc.TypeDef, fn valuerpc.Chat) error {
	if name == "" || fn == nil {
		return fmt.Errorf("valueclient: AddChat requires a name and a handler")
	}
	t.functionMap.Store(name, &clientFunction{ft: cfnChat, args: args, chat: fn})
	return nil
}

func (t *rpcClient) deleteServing(reqId value.Number) {
	t.servingMap.Delete(reqId.Long())
}

// streamKindFor maps a stream-open message type to the registered handler kind.
func streamKindFor(mt valuerpc.MessageType) clientFnKind {
	switch mt {
	case valuerpc.GetStreamRequest:
		return cfnOut
	case valuerpc.PutStreamRequest:
		return cfnIn
	default: // ChatRequest
		return cfnChat
	}
}

// serveInboundStream handles a server-opened stream request (reverse stream):
// it registers a serving request synchronously (so later frames for it route
// correctly), then runs the handler off the response loop.
func (t *rpcClient) serveInboundStream(msgType valuerpc.MessageType, req value.Map) {
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
	if cf.ft != streamKindFor(msgType) {
		t.reply(valuerpc.NewFunctionError(reqId, valuerpc.CodeInvalidArgument, "function '%s' wrong stream type", name.String()))
		return
	}
	args := req.Get(valuerpc.DefaultDialect.ArgumentsField)
	if !valuerpc.Verify(args, cf.args) {
		t.reply(valuerpc.NewFunctionError(reqId, valuerpc.CodeInvalidArgument, "stream '%s' invalid args", name.String()))
		return
	}

	// Register synchronously on the response loop so a frame the server sends in
	// response to the initial credit grant always finds the serving request.
	ctx, cancel := context.WithCancel(t.baseCtx)
	sr := newClientServingRequest(cf.ft, reqId)
	sr.cancel = cancel
	if cf.ft == cfnIn || cf.ft == cfnChat {
		sr.setupInbound(t.maxPending, t)
	}
	t.servingMap.Store(reqId.Long(), sr)
	if cf.ft == cfnIn || cf.ft == cfnChat {
		sr.grantInitialInbound(t)
	}

	// The handler may block briefly; run it off the single response loop.
	go func() {
		switch cf.ft {
		case cfnOut:
			outC, err := cf.outStream(ctx, args)
			if err != nil {
				sr.closeRequest(t)
				t.reply(valuerpc.NewHandlerError(reqId, "out stream "+name.String(), err))
				return
			}
			sr.outgoingStreamer(outC, t)
		case cfnIn:
			if err := cf.inStream(ctx, args, sr.inC); err != nil {
				sr.closeRequest(t)
				t.reply(valuerpc.NewHandlerError(reqId, "in stream "+name.String(), err))
				return
			}
			t.reply(valuerpc.NewStreamReady(reqId))
		case cfnChat:
			outC, err := cf.chat(ctx, args, sr.inC)
			if err != nil {
				sr.closeRequest(t)
				t.reply(valuerpc.NewHandlerError(reqId, "chat "+name.String(), err))
				return
			}
			sr.outgoingStreamer(outC, t)
		}
	}()
}

// clientServingRequest is the client-side responder for a server-initiated
// stream. It mirrors the server's servingRequest, sending frames back through
// the client and reusing the shared StreamPump/CreditGate for flow control.
type clientServingRequest struct {
	ft        clientFnKind
	requestId value.Number

	inC        chan value.Value     // handler-facing inbound channel (pump output); in/chat
	inPump     *valuerpc.StreamPump // drains into inC; nil for outgoing
	sendCredit *valuerpc.CreditGate // gates client->server StreamValue sends; out/chat
	cancel     context.CancelFunc

	creditWindow     int64
	inboundDelivered int64 // pump goroutine only

	closed   atomic.Bool
	inClosed atomic.Bool
	done     chan struct{}
}

func newClientServingRequest(ft clientFnKind, reqId value.Number) *clientServingRequest {
	sr := &clientServingRequest{ft: ft, requestId: reqId, done: make(chan struct{})}
	if ft == cfnOut || ft == cfnChat {
		sr.sendCredit = valuerpc.NewCreditGate()
	}
	return sr
}

func (sr *clientServingRequest) setupInbound(maxPending int, t *rpcClient) {
	if maxPending <= 0 {
		maxPending = valuerpc.DefaultMaxPending
	}
	sr.creditWindow = int64(maxPending)
	sr.inC = make(chan value.Value, maxPending)
	sr.inPump = valuerpc.NewStreamPump(sr.inC, maxPending, func() { sr.grantInbound(t) })
}

func (sr *clientServingRequest) grantInitialInbound(t *rpcClient) {
	t.reply(valuerpc.NewStreamCredit(sr.requestId, sr.creditWindow))
}

func (sr *clientServingRequest) grantInbound(t *rpcClient) {
	sr.inboundDelivered++
	batch := sr.creditWindow / 2
	if batch < 1 {
		batch = 1
	}
	if sr.inboundDelivered >= batch {
		n := sr.inboundDelivered
		sr.inboundDelivered = 0
		t.reply(valuerpc.NewStreamCredit(sr.requestId, n))
	}
}

func (sr *clientServingRequest) cancelCtx() {
	if sr.cancel != nil {
		sr.cancel()
	}
}

func (sr *clientServingRequest) closeInboundChan() {
	if sr.inClosed.CompareAndSwap(false, true) {
		if sr.inPump != nil {
			sr.inPump.Close()
		}
	}
}

func (sr *clientServingRequest) Close() {
	if sr.closed.CompareAndSwap(false, true) {
		close(sr.done)
		sr.cancelCtx()
		if sr.sendCredit != nil {
			sr.sendCredit.Close()
		}
		sr.inClosed.Store(true)
		if sr.inPump != nil {
			sr.inPump.Stop()
		}
	}
}

func (sr *clientServingRequest) closeRequest(t *rpcClient) {
	t.deleteServing(sr.requestId)
	sr.Close()
}

func (sr *clientServingRequest) serveRunning(msgType valuerpc.MessageType, req value.Map, t *rpcClient) {
	switch msgType {

	case valuerpc.CancelRequest:
		sr.closeRequest(t)

	case valuerpc.StreamValue:
		if v := req.Get(valuerpc.DefaultDialect.ValueField); v != nil && v.Kind() != value.NULL && sr.inPump != nil {
			sr.inPump.Push(v)
		}
		// A peer that ignores its credit and overruns the pump truncates the
		// stream; surface it instead of silently dropping values.
		if sr.inPump != nil && sr.inPump.Overflowed() {
			t.reply(valuerpc.NewFunctionError(sr.requestId, valuerpc.CodeResourceExhausted,
				"inbound stream %d truncated: peer exceeded flow-control credit", sr.requestId.Long()))
			sr.closeRequest(t)
		}

	case valuerpc.StreamEnd:
		if v := req.Get(valuerpc.DefaultDialect.ValueField); v != nil && v.Kind() != value.NULL && sr.inPump != nil {
			sr.inPump.Push(v)
		}
		// For chat the peer ending its input must not tear down our output side;
		// just close the inbound channel. For a pure incoming stream, end input
		// and retire the request.
		sr.closeInboundChan()
		if sr.ft != cfnChat {
			t.deleteServing(sr.requestId)
			sr.cancelCtx()
		}

	case valuerpc.StreamCredit:
		if cr, ok := valuerpc.GetNumberField(req, valuerpc.DefaultDialect.CreditField); ok && sr.sendCredit != nil {
			sr.sendCredit.Grant(cr.Long())
		}
	}
}

// outgoingStreamer drains the handler's outC to the server (outgoing/chat),
// gated by the server's credit, then sends StreamEnd.
func (sr *clientServingRequest) outgoingStreamer(outC <-chan value.Value, t *rpcClient) {
	t.reply(valuerpc.NewStreamReady(sr.requestId))
	for {
		var val value.Value
		var ok bool
		select {
		case val, ok = <-outC:
		case <-sr.done:
			ok = false
		}
		if !ok || sr.closed.Load() {
			t.reply(valuerpc.NewStreamEnd(sr.requestId, val))
			sr.closeRequest(t)
			break
		}
		if sr.sendCredit != nil && !sr.sendCredit.Acquire() {
			sr.closeRequest(t)
			break
		}
		t.reply(valuerpc.NewStreamValue(sr.requestId, val))
	}
}
