/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"time"

	"github.com/pkg/errors"
	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/atomic"
)

var IncomingQueueCap = 4096

type servingRequest struct {
	ft               functionType
	requestId        value.Number
	inC              chan value.Value
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
		sr.inC = make(chan value.Value, IncomingQueueCap)
	}

	return sr
}

func (t *servingRequest) Close() {
	if t.closed.CAS(false, true) {
		close(t.done)
		t.closeInboundChan()
	}
}

// closeInboundChan closes the application's inbound channel exactly once,
// signalling end-of-input to the handler without tearing down the whole
// request (used for chat half-close).
func (t *servingRequest) closeInboundChan() {
	if t.inClosed.CAS(false, true) {
		if t.inC != nil {
			close(t.inC)
		}
	}
}

// offer hands a value to the application's inbound channel without panicking if
// the request was closed concurrently (BUG-6) and without blocking forever
// after close. It still blocks while a live consumer is slow (backpressure).
func (t *servingRequest) offer(val value.Value) bool {
	defer func() { _ = recover() }() // inC may be closed concurrently
	select {
	case t.inC <- val:
		return true
	case <-t.done:
		return false
	}
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
		t.throttleOutgoing.Inc()

	case vrpc.ThrottleDecrease:
		t.throttleOutgoing.Dec()

	default:
		return errors.Errorf("unknown message type in %s", req.String())

	}

	return nil

}

func (t *servingRequest) incomingStreamValue(req value.Map) error {

	if t.inC == nil {
		return errors.Errorf("incoming value stream not found in serving request for %d", t.requestId)
	}

	if val := req.Get(vrpc.ValueField); val != value.Null {
		t.offer(val)
	}

	return nil
}

func (t *servingRequest) incomingStreamEnd(req value.Map, cli *servingClient) error {

	if t.inC == nil {
		return errors.Errorf("incoming end stream not found in serving request for %d", t.requestId)
	}

	if val := req.Get(vrpc.ValueField); val != value.Null {
		t.offer(val)
	}

	// For chat, the client ending its input must NOT tear down the server's
	// output side (you can half-close the send direction and keep receiving).
	// Just close the inbound channel; the request is torn down when the
	// outgoing stream finishes. For a pure incoming stream there is no output,
	// so close the whole request.
	if t.ft == chat {
		t.closeInboundChan()
		return nil
	}
	return t.closeRequest(cli)
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
