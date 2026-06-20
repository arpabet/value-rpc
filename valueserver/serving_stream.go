/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

// StreamEstablishTimeout bounds how long a server-initiated stream waits for the
// client's StreamReady ack before giving up.
var StreamEstablishTimeout = 10 * time.Second

// streamKind names the three streaming patterns from the *initiator's* point of
// view (here, the server initiating toward the client).
type streamKind int

const (
	getStreamKind streamKind = iota // we receive a stream from the peer
	putStreamKind                   // we send a stream to the peer
	chatKind                        // both
)

// nextServerRequestId draws the next server-initiated (negative) request id,
// disjoint from the client's positive ids and the handshake's 0. Shared by
// reverse unary calls and reverse streams so the two never collide.
func (t *servingClient) nextServerRequestId() int64 {
	return -t.lastServerRequest.Add(1)
}

func (t *servingClient) findPendingStream(reqId value.Number) (*clientStream, bool) {
	if v, ok := t.pendingStreams.Load(reqId.Long()); ok {
		return v.(*clientStream), true
	}
	return nil, false
}

// clientStream tracks one server-initiated stream toward the client. It mirrors
// the client's initiator-side rpcRequestCtx, adapted to send through the serving
// client; the shared StreamPump/CreditGate carry the flow control.
type clientStream struct {
	requestId int64
	kind      streamKind

	resultCh   chan value.Value
	resultPump *vrpc.StreamPump // get/chat: drains client->server values to the caller; nil for put
	sendCredit *vrpc.CreditGate // put/chat: gates server->client StreamValue sends; nil for get
	done       chan struct{}
	closeOnce  sync.Once

	getClosed atomic.Bool
	putClosed atomic.Bool
	resultErr atomic.Pointer[error]

	creditWindow  int64
	recvDelivered int64       // pump goroutine only
	grantCredit   func(int64) // sends a StreamCredit to the client
}

func newClientStream(reqId int64, kind streamKind, receiveCap, maxPending int, grantCredit func(int64)) *clientStream {
	t := &clientStream{
		requestId:   reqId,
		kind:        kind,
		resultCh:    make(chan value.Value, receiveCap),
		done:        make(chan struct{}),
		grantCredit: grantCredit,
	}
	if kind == getStreamKind || kind == chatKind {
		if maxPending <= 0 {
			maxPending = vrpc.DefaultMaxPending
		}
		t.creditWindow = int64(maxPending)
		// Start at -1 so the StreamReady ack delivery (which consumes no send
		// credit) brings recvDelivered to 0 rather than counting toward the first
		// credit grant (mirrors the client initiator).
		t.recvDelivered = -1
		t.resultPump = vrpc.NewStreamPump(t.resultCh, maxPending, t.grantRecv)
	}
	if kind == putStreamKind || kind == chatKind {
		t.sendCredit = vrpc.NewCreditGate()
	}
	return t
}

func (t *clientStream) sendInitialCredit() {
	if t.resultPump != nil && t.grantCredit != nil {
		t.grantCredit(t.creditWindow)
	}
}

func (t *clientStream) grantRecv() {
	t.recvDelivered++
	batch := t.creditWindow / 2
	if batch < 1 {
		batch = 1
	}
	if t.recvDelivered >= batch {
		n := t.recvDelivered
		t.recvDelivered = 0
		if t.grantCredit != nil {
			t.grantCredit(n)
		}
	}
}

// notifyResult delivers a client->server value (or the StreamReady nil) to the
// caller without blocking the connection read loop.
func (t *clientStream) notifyResult(res value.Value) bool {
	if t.resultPump != nil {
		return t.resultPump.Push(res)
	}
	defer func() { _ = recover() }() // resultCh may close concurrently
	select {
	case t.resultCh <- res:
		return true
	case <-t.done:
		return false
	}
}

func (t *clientStream) closeResult() {
	t.closeOnce.Do(func() {
		close(t.done)
		if t.sendCredit != nil {
			t.sendCredit.Close()
		}
		if t.resultPump != nil {
			t.resultPump.Close()
		} else {
			close(t.resultCh)
		}
	})
}

func (t *clientStream) Close() {
	t.getClosed.Store(true)
	t.putClosed.Store(true)
	if t.resultPump != nil {
		t.resultPump.Stop()
	}
	t.closeResult()
}

func (t *clientStream) IsPutOpen() bool { return !t.putClosed.Load() }

func (t *clientStream) setError(err error) { t.resultErr.Store(&err) }
func (t *clientStream) err(def error) error {
	if e := t.resultErr.Load(); e != nil {
		return *e
	}
	return def
}

// tryGetClose marks the client->server side finished and closes resultCh.
// Returns true when the whole stream is finished (for chat, only once the
// server->client side has also finished).
func (t *clientStream) tryGetClose() bool {
	t.getClosed.Store(true)
	t.closeResult()
	if t.kind == chatKind {
		return t.putClosed.Load()
	}
	return true
}

// tryPutClose marks the server->client (streamOut) side finished. Terminal for a
// put-stream; for chat the get side owns the close.
func (t *clientStream) tryPutClose() bool {
	t.putClosed.Store(true)
	if t.kind == putStreamKind {
		t.closeResult()
		return true
	}
	return t.getClosed.Load()
}

// handleInbound processes a frame the client sent for this server-initiated
// stream. Runs on the connection read loop, so it never blocks.
func (t *clientStream) handleInbound(msgType vrpc.MessageType, req value.Map, cli *servingClient) error {
	switch msgType {

	case vrpc.StreamReady:
		t.notifyResult(nil) // wake the establisher
		return nil

	case vrpc.StreamValue:
		if v := req.Get(vrpc.DefaultDialect.ValueField); v != nil && v.Kind() != value.NULL {
			if !t.notifyResult(v) && t.resultPump != nil && t.resultPump.Overflowed() {
				// The client overran our buffer; surface it and tear down.
				cli.send(vrpc.NewFunctionError(value.Long(t.requestId), vrpc.CodeResourceExhausted,
					"reverse stream %d truncated: peer exceeded flow-control credit", t.requestId))
				cli.deletePendingStream(t.requestId)
				t.Close()
			}
		}
		return nil

	case vrpc.StreamEnd:
		if v := req.Get(vrpc.DefaultDialect.ValueField); v != nil && v.Kind() != value.NULL {
			t.notifyResult(v)
		}
		if t.tryGetClose() {
			cli.deletePendingStream(t.requestId)
		}
		return nil

	case vrpc.StreamCredit:
		if cr, ok := vrpc.GetNumberField(req, vrpc.DefaultDialect.CreditField); ok && t.sendCredit != nil {
			t.sendCredit.Grant(cr.Long())
		}
		return nil

	case vrpc.ErrorResponse:
		code := vrpc.CodeUnknown
		if c, ok := vrpc.GetNumberField(req, vrpc.DefaultDialect.CodeField); ok {
			code = vrpc.Code(c.Long())
		}
		msg := ""
		if s, ok := vrpc.GetStringField(req, vrpc.DefaultDialect.ErrorField); ok {
			msg = s.String()
		}
		t.setError(&vrpc.Error{Code: code, Message: msg})
		cli.deletePendingStream(t.requestId)
		t.Close()
		return nil

	default:
		return nil
	}
}

func (t *servingClient) deletePendingStream(reqId int64) {
	t.pendingStreams.Delete(reqId)
}

// awaitReady blocks until the client acks the stream (StreamReady, delivered as a
// nil on resultCh), the context is cancelled, or the establish timeout elapses.
func (t *servingClient) awaitReady(ctx context.Context, cs *clientStream) error {
	timer := time.NewTimer(StreamEstablishTimeout)
	defer timer.Stop()
	select {
	case _, ok := <-cs.resultCh:
		if !ok {
			return cs.err(vrpc.NewError(vrpc.CodeUnavailable, "reverse stream closed before ready"))
		}
		return nil
	case <-ctx.Done():
		t.cancelClientStream(cs)
		return ctx.Err()
	case <-t.done:
		t.cancelClientStream(cs)
		return vrpc.ErrClientClosed
	case <-timer.C:
		t.cancelClientStream(cs)
		if err := ctx.Err(); err != nil {
			return err
		}
		return vrpc.NewError(vrpc.CodeDeadlineExceeded, "reverse stream %d: client did not ack", cs.requestId)
	}
}

// cancelClientStream tells the client to cancel the stream and tears down local
// state.
func (t *servingClient) cancelClientStream(cs *clientStream) {
	_ = t.send(value.EmptyMap(true).
		Put(vrpc.DefaultDialect.MessageTypeField, vrpc.CancelRequest.Long()).
		Put(vrpc.DefaultDialect.RequestIdField, value.Long(cs.requestId)))
	t.deletePendingStream(cs.requestId)
	cs.Close()
}

// watchStream tears the stream down if ctx is cancelled before it finishes.
func (t *servingClient) watchStream(ctx context.Context, cs *clientStream) {
	if ctx.Done() == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			t.cancelClientStream(cs)
		case <-cs.done:
		case <-t.done:
		}
	}()
}

// openStream allocates the request, registers it, sends the open frame, and waits
// for the client's StreamReady ack.
func (t *servingClient) openStream(ctx context.Context, mt vrpc.MessageType, kind streamKind, name string, args value.Value, receiveCap int) (*clientStream, error) {
	reqId := t.nextServerRequestId()
	cs := newClientStream(reqId, kind, receiveCap, vrpc.DefaultMaxPending,
		func(n int64) { _ = t.send(vrpc.NewStreamCredit(value.Long(reqId), n)) })
	t.pendingStreams.Store(reqId, cs) // register BEFORE sending so StreamReady finds it

	if err := t.send(vrpc.NewStreamRequest(mt, reqId, name, args)); err != nil {
		t.deletePendingStream(reqId)
		cs.Close()
		return nil, err
	}
	if err := t.awaitReady(ctx, cs); err != nil {
		return nil, err
	}
	return cs, nil
}

// GetStream opens a server->client stream the server *receives* values on: the
// client serves it with AddOutgoingStream.
func (t *servingClient) GetStream(ctx context.Context, name string, args value.Value, receiveCap int) (<-chan value.Value, int64, error) {
	cs, err := t.openStream(ctx, vrpc.GetStreamRequest, getStreamKind, name, args, receiveCap)
	if err != nil {
		return nil, 0, err
	}
	cs.sendInitialCredit()
	t.watchStream(ctx, cs)
	return cs.resultCh, cs.requestId, nil
}

// PutStream opens a server->client stream the server *sends* values on: the
// client serves it with AddIncomingStream.
func (t *servingClient) PutStream(ctx context.Context, name string, args value.Value, putCh <-chan value.Value) error {
	cs, err := t.openStream(ctx, vrpc.PutStreamRequest, putStreamKind, name, args, 1)
	if err != nil {
		return err
	}
	t.watchStream(ctx, cs)
	go t.streamOut(cs, putCh)
	return nil
}

// Chat opens a bidirectional server->client stream: the client serves it with
// AddChat.
func (t *servingClient) Chat(ctx context.Context, name string, args value.Value, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error) {
	cs, err := t.openStream(ctx, vrpc.ChatRequest, chatKind, name, args, receiveCap+1)
	if err != nil {
		return nil, 0, err
	}
	cs.sendInitialCredit()
	t.watchStream(ctx, cs)
	go t.streamOut(cs, putCh)
	return cs.resultCh, cs.requestId, nil
}

// streamOut drains the caller's putCh to the client (put/chat), gated by the
// client's credit, and sends StreamEnd when the channel closes.
func (t *servingClient) streamOut(cs *clientStream, putCh <-chan value.Value) {
	for cs.IsPutOpen() {
		val, ok := <-putCh
		if !ok {
			_ = t.send(vrpc.NewStreamEnd(value.Long(cs.requestId), nil))
			break
		}
		if cs.sendCredit != nil && !cs.sendCredit.Acquire() {
			break
		}
		_ = t.send(vrpc.NewStreamValue(value.Long(cs.requestId), val))
	}
	if cs.tryPutClose() {
		t.deletePendingStream(cs.requestId)
	}
}
