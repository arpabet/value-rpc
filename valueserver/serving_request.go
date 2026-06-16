/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

var IncomingQueueCap = 4096

type servingRequest struct {
	ft               functionType
	requestId        value.Number
	inC              chan value.Value // handler-facing inbound channel (pump output)
	inPump           *vrpc.StreamPump // drains into inC; nil for non-inbound requests
	throttleOutgoing atomic.Int64

	closed   atomic.Bool
	inClosed atomic.Bool
	done     chan struct{}
}

func NewServingRequest(ft functionType, requestId value.Number) *servingRequest {

	sr := &servingRequest{
		ft:        ft,
		requestId: requestId,
		done:      make(chan struct{}),
	}

	if ft == incomingStream || ft == chat {
		// The connection read loop feeds inPump (non-blocking); the pump
		// goroutine delivers into inC at the handler's pace, so a slow handler
		// can no longer stall the whole connection (BUG-6).
		sr.inC = make(chan value.Value, IncomingQueueCap)
		sr.inPump = vrpc.NewStreamPump(sr.inC, 0)
	}

	return sr
}

func (t *servingRequest) Close() {
	if t.closed.CompareAndSwap(false, true) {
		close(t.done)
		// Hard teardown: abandon any buffered inbound values and unblock a pump
		// stuck delivering to a handler that stopped reading.
		t.inClosed.Store(true)
		if t.inPump != nil {
			t.inPump.Stop()
		}
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
		return t.closeRequest(cli)

	case vrpc.StreamValue:
		return t.incomingStreamValue(req)

	case vrpc.StreamEnd:
		return t.incomingStreamEnd(req, cli)

	case vrpc.ThrottleIncrease:
		t.throttleOutgoing.Add(1)

	case vrpc.ThrottleDecrease:
		t.throttleOutgoing.Add(-1)

	default:
		return errors.Errorf("unknown message type in %s", req.String())

	}

	return nil

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
	cli.canceledRequests.Delete(t.requestId.Long())
	return nil
}

func (t *servingRequest) closeRequest(cli *servingClient) error {
	cli.deleteRequest(t.requestId)
	t.Close()
	// BUG-13 fix: canceledRequests is keyed by int64 (reqId.Long()), not by the
	// value.Number; deleting with the wrong key type leaked entries forever.
	cli.canceledRequests.Delete(t.requestId.Long())
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

		cli.send(StreamValue(t.requestId, val))

		th := t.throttleOutgoing.Load()
		if th > 0 {
			time.Sleep(time.Millisecond * time.Duration(th))
		}

	}

}
