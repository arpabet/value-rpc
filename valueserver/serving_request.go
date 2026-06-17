/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

var IncomingQueueCap = 4096

type servingRequest struct {
	ft         functionType
	requestId  value.Number
	inC        chan value.Value   // handler-facing inbound channel (pump output)
	inPump     *vrpc.StreamPump   // drains into inC; nil for non-inbound requests
	sendCredit *vrpc.CreditGate   // gates server->client StreamValue sends (outgoing/chat)
	cancel     context.CancelFunc // cancels the handler's request context on teardown

	creditWindow     int64 // inbound flow-control window granted to the client
	inboundDelivered int64 // values delivered since last credit grant; pump goroutine only

	method string       // function name, for metrics
	start  time.Time    // request start, for metrics latency
	code   atomic.Int32 // terminal vrpc.Code for metrics; 0 (CodeOK) unless set on a failure path

	closed   atomic.Bool
	inClosed atomic.Bool
	done     chan struct{}
}

// setCode records the terminal outcome code for metrics (first non-OK wins).
func (t *servingRequest) setCode(c vrpc.Code) {
	t.code.CompareAndSwap(int32(vrpc.CodeOK), int32(c))
}

func NewServingRequest(ft functionType, requestId value.Number) *servingRequest {
	sr := &servingRequest{
		ft:        ft,
		requestId: requestId,
		done:      make(chan struct{}),
	}
	if ft == outgoingStream || ft == chat {
		// The server sends stream values; the gate blocks the streamer until the
		// client grants credit (credit-based flow control).
		sr.sendCredit = vrpc.NewCreditGate()
	}
	return sr
}

// setupInbound creates the inbound pump (incoming-stream/chat) and grants the
// client an initial credit window. The read loop feeds inPump without blocking
// (BUG-6); the pump's deliver hook replenishes the client's send credit as the
// handler consumes, so a fast client can never overrun the buffer — lossless,
// bounded, non-HOL flow control.
func (t *servingRequest) setupInbound(cli *servingClient, incomingQueueCap, maxPending int) {
	t.creditWindow = int64(maxPending)
	t.inC = make(chan value.Value, incomingQueueCap)
	t.inPump = vrpc.NewStreamPump(t.inC, maxPending, func() { t.grantInbound(cli) })
	cli.send(vrpc.NewStreamCredit(t.requestId, t.creditWindow))
}

// grantInbound replenishes the client's send credit as the handler consumes
// inbound values. Called once per value the pump delivers (pump goroutine only,
// so the counter needs no lock); grants are batched to ~half the window.
func (t *servingRequest) grantInbound(cli *servingClient) {
	t.inboundDelivered++
	batch := t.creditWindow / 2
	if batch < 1 {
		batch = 1
	}
	if t.inboundDelivered >= batch {
		n := t.inboundDelivered
		t.inboundDelivered = 0
		_ = cli.send(vrpc.NewStreamCredit(t.requestId, n))
	}
}

func (t *servingRequest) Close() {
	if t.closed.CompareAndSwap(false, true) {
		close(t.done)
		t.cancelCtx()
		if t.sendCredit != nil {
			t.sendCredit.Close() // unblock a streamer waiting for credit
		}
		// Hard teardown: abandon any buffered inbound values and unblock a pump
		// stuck delivering to a handler that stopped reading.
		t.inClosed.Store(true)
		if t.inPump != nil {
			t.inPump.Stop()
		}
	}
}

// cancelCtx cancels the handler's request context. Safe to call repeatedly and
// when no context was attached (e.g. an outgoing stream torn down before setup).
func (t *servingRequest) cancelCtx() {
	if t.cancel != nil {
		t.cancel()
	}
}

// closeInboundChan signals end-of-input to the handler exactly once: the pump
// drains whatever is buffered, then closes inC. It does not tear down the whole
// request (used for chat half-close and the normal end of a client stream).
func (t *servingRequest) closeInboundChan() {
	if t.inClosed.CompareAndSwap(false, true) {
		if t.inPump != nil {
			t.inPump.Close()
		}
	}
}

// offer hands a value to the inbound pump. It never blocks the connection read
// loop (BUG-6): Push enqueues and returns immediately. It returns false if the
// request is closed or the consumer fell too far behind (overflow).
func (t *servingRequest) offer(val value.Value) bool {
	if t.inPump == nil {
		return false
	}
	return t.inPump.Push(val)
}

func (t *servingRequest) serveRunningRequest(msgType vrpc.MessageType, req value.Map, cli *servingClient) error {

	switch msgType {

	case vrpc.CancelRequest:
		t.setCode(vrpc.CodeCanceled)
		return t.closeRequest(cli)

	case vrpc.StreamValue:
		if err := t.incomingStreamValue(req); err != nil {
			return err
		}
		cli.cfg.metrics.StreamValue(t.method)
		// Credit-based flow control keeps a cooperating client within the buffer.
		// A client that ignores its credit and overruns the pump truncates the
		// stream; surface it (tell the client, tear down) instead of silently
		// dropping values (#13).
		if t.inPump != nil && t.inPump.Overflowed() {
			cli.send(FunctionError(t.requestId, vrpc.CodeResourceExhausted, "inbound stream %d truncated: client exceeded flow-control credit", t.requestId.Long()))
			t.setCode(vrpc.CodeResourceExhausted)
			return t.closeRequest(cli)
		}
		return nil

	case vrpc.StreamEnd:
		return t.incomingStreamEnd(req, cli)

	case vrpc.StreamCredit:
		// The client granted the server's outgoing stream more credit.
		if cr, ok := vrpc.GetNumberField(req, vrpc.CreditField); ok && t.sendCredit != nil {
			t.sendCredit.Grant(cr.Long())
		}
		return nil

	default:
		return errors.Errorf("unknown message type in %s", req.String())

	}

}

func (t *servingRequest) incomingStreamValue(req value.Map) error {

	if t.inPump == nil {
		return errors.Errorf("incoming value stream not found in serving request for %d", t.requestId)
	}

	if val := req.Get(vrpc.ValueField); val != value.Null {
		t.offer(val)
	}

	return nil
}

func (t *servingRequest) incomingStreamEnd(req value.Map, cli *servingClient) error {

	if t.inPump == nil {
		return errors.Errorf("incoming end stream not found in serving request for %d", t.requestId)
	}

	if val := req.Get(vrpc.ValueField); val != value.Null {
		t.offer(val)
		cli.cfg.metrics.StreamValue(t.method)
	}

	// For chat, the client ending its input must NOT tear down the server's
	// output side (you can half-close the send direction and keep receiving).
	// Just close the inbound channel; the request is torn down when the
	// outgoing stream finishes.
	if t.ft == chat {
		t.closeInboundChan()
		return nil
	}

	// For a pure incoming stream there is no server output. End the input
	// gracefully (closeInboundChan drains buffered values to the handler, then
	// closes inC) and retire the request. We must NOT hard-Close here: that
	// would Stop the pump and drop values a lagging consumer has not read yet.
	t.closeInboundChan()
	cli.deleteRequest(t.requestId)
	t.cancelCtx() // end of client input: release the handler's request context
	return nil
}

func (t *servingRequest) closeRequest(cli *servingClient) error {
	cli.deleteRequest(t.requestId)
	t.Close() // also cancels the handler's request context
	return nil
}

func (t *servingRequest) outgoingStreamer(outC <-chan value.Value, cli *servingClient) {

	cli.send(StreamReady(t.requestId))

	for {

		var val value.Value
		var ok bool
		select {
		case val, ok = <-outC:
		case <-t.done: // client/request closed: stop promptly
			ok = false
		}

		if !ok || t.closed.Load() {
			cli.send(StreamEnd(t.requestId, val))
			// The server's output ending is the terminal event for both
			// outgoing-stream and chat requests; tear the request down here
			// (closeRequest is idempotent).
			t.closeRequest(cli)
			break
		}

		// Credit-based flow control: wait for the client to have buffer space
		// before sending. Only this streamer goroutine blocks — never the shared
		// connection loop. A closed gate (teardown) ends the stream.
		if t.sendCredit != nil && !t.sendCredit.Acquire() {
			t.closeRequest(cli)
			break
		}

		cli.send(StreamValue(t.requestId, val))
		cli.cfg.metrics.StreamValue(t.method)

	}

}
