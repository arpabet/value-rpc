/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"golang.org/x/xerrors"
)

type streamKind int

const (
	unaryKind streamKind = iota
	getStreamKind
	putStreamKind
	chatKind
)

// rpcRequestCtx tracks a single in-flight request.
//
// resultCh carries server->client values. It is closed exactly once
// (closeOnce), on the *get-side* terminal event (FunctionResponse /
// ErrorResponse / StreamEnd) for unary/get/chat requests, and on the *put-side*
// terminal event (streamOut finished) for put-stream requests — which have no
// server->client terminal. This is the fix for BUG-7, where both the get and
// put paths used to close resultCh and a chat reliably double-closed it.
type rpcRequestCtx struct {
	requestId int64
	kind      streamKind
	req       value.Map
	start     time.Time

	resultCh   chan value.Value
	resultPump *valuerpc.StreamPump // drains into resultCh for streaming kinds; nil for unary/put
	sendCredit *valuerpc.CreditGate // gates client->server streamOut (put/chat); nil otherwise
	done       chan struct{}        // closed together with resultCh
	closeOnce  sync.Once
	cancelOnce sync.Once // bounds a slow-consumer cancel to one per request

	getClosed atomic.Bool // no more server->client values will be delivered
	putClosed atomic.Bool // client->server side (streamOut) has finished

	resultErr atomic.Pointer[error] // stdlib has no atomic.Error

	creditWindow  int64       // server->client receive window granted to the server
	recvDelivered int64       // values delivered since last grant; pump goroutine only
	grantCredit   func(int64) // sends a StreamCredit to the server (set by the client)

	metrics valuerpc.Metrics // set by the client; RequestEnd fires once at teardown
}

func NewRequestCtx(requestId int64, kind streamKind, req value.Map, receiveCap, maxPending int, grantCredit func(int64)) *rpcRequestCtx {
	t := &rpcRequestCtx{
		requestId:   requestId,
		kind:        kind,
		req:         req,
		start:       time.Now(),
		resultCh:    make(chan value.Value, receiveCap),
		done:        make(chan struct{}),
		grantCredit: grantCredit,
	}
	// Server->client streams (get-stream, chat) deliver through a pump so a slow
	// consumer cannot block the single response loop and every other request on
	// the connection (BUG-6). The pump's deliver hook replenishes the server's
	// send credit as the consumer drains (credit-based flow control). Unary and
	// put-stream receive a single value into a cap>=1 buffer and never
	// head-of-line block, so they keep the direct path.
	if kind == getStreamKind || kind == chatKind {
		if maxPending <= 0 {
			maxPending = valuerpc.DefaultMaxPending
		}
		t.creditWindow = int64(maxPending)
		// The server sends a StreamReady ack before acquiring any credit, and the
		// client delivers it through the pump (as a nil) to wake the establisher.
		// That delivery consumed no send credit, so it must not count toward credit
		// replenishment — otherwise the first batch grants the server one slot more
		// than the window, and a slow consumer overruns the pump by one (a spurious
		// "exceeded flow-control credit" truncation under load). Start at -1 so the
		// ack delivery brings recvDelivered to 0; absent an ack this merely delays
		// the first grant by one (still safe).
		t.recvDelivered = -1
		t.resultPump = valuerpc.NewStreamPump(t.resultCh, maxPending, t.grantRecv)
	}
	// Client->server streams (put, chat) send through a credit gate granted by
	// the server.
	if kind == putStreamKind || kind == chatKind {
		t.sendCredit = valuerpc.NewCreditGate()
	}
	return t
}

// sendInitialCredit grants the server its initial send window for the
// server->client direction. Called once the stream is established.
func (t *rpcRequestCtx) sendInitialCredit() {
	if t.resultPump != nil && t.grantCredit != nil {
		t.grantCredit(t.creditWindow)
	}
}

// grantRecv replenishes the server's send credit as the consumer drains values.
// Called once per delivery (pump goroutine only); grants are batched to ~half
// the window.
func (t *rpcRequestCtx) grantRecv() {
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

func (t *rpcRequestCtx) Name() string {
	fn := t.req.GetString(valuerpc.DefaultDialect.FunctionNameField)
	if fn != nil {
		return fn.String()
	}
	return "unknown"
}

func (t *rpcRequestCtx) Stats() (int, int) {
	return len(t.resultCh), cap(t.resultCh)
}

func (t *rpcRequestCtx) Elapsed() int64 {
	elapsed := time.Now().Sub(t.start)
	return elapsed.Microseconds()
}

// notifyResult delivers a server->client value. For streaming kinds it hands the
// value to the pump (non-blocking, so the response loop never stalls on a slow
// consumer — BUG-6); for unary/put it does a guarded send into the cap>=1
// buffer. It returns false when the value could not be queued (request finished
// or the consumer fell too far behind).
func (t *rpcRequestCtx) notifyResult(res value.Value) bool {
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

// closeResult closes resultCh (and done) exactly once, draining any buffered
// stream values to the consumer first.
func (t *rpcRequestCtx) closeResult() {
	t.closeOnce.Do(func() {
		close(t.done)
		if t.sendCredit != nil {
			t.sendCredit.Close() // unblock streamOut waiting for credit
		}
		if t.resultPump != nil {
			t.resultPump.Close() // drain, then close resultCh
		} else {
			close(t.resultCh)
		}
		// Single end-of-request point: drives the request/error counters,
		// in-flight gauge, and latency (paired with RequestBegin at creation).
		if t.metrics != nil {
			t.metrics.RequestEnd(t.Name(), metricsCode(t.Error(nil)), time.Since(t.start))
		}
	})
}

// metricsCode maps a request's terminal error to a Code for metrics: the code of
// a *valuerpc.Error, DeadlineExceeded/Canceled for the client's timeout/cancel
// sentinels, CodeOK for success.
func metricsCode(err error) valuerpc.Code {
	switch {
	case err == nil:
		return valuerpc.CodeOK
	case xerrors.Is(err, ErrTimeoutError), xerrors.Is(err, context.DeadlineExceeded):
		return valuerpc.CodeDeadlineExceeded
	case xerrors.Is(err, context.Canceled):
		return valuerpc.CodeCanceled
	default:
		return valuerpc.CodeOf(err)
	}
}

// Close force-closes the request: unary completion, error, or cancellation. For
// a streaming kind it stops the pump so a consumer that stopped reading cannot
// leak the pump goroutine.
func (t *rpcRequestCtx) Close() {
	t.getClosed.Store(true)
	t.putClosed.Store(true)
	if t.resultPump != nil {
		t.resultPump.Stop()
	}
	t.closeResult() // also closes the send-credit gate
}

func (t *rpcRequestCtx) IsGetOpen() bool {
	return !t.getClosed.Load()
}

func (t *rpcRequestCtx) IsPutOpen() bool {
	return !t.putClosed.Load()
}

// TryGetClose marks the server->client side finished and closes resultCh (which
// carries that side). Returns true when the whole request is finished, so the
// caller can delete it from the request map. For chat we keep the entry until
// the put side also finishes, so late throttle acks can still be routed.
func (t *rpcRequestCtx) TryGetClose() bool {
	t.getClosed.Store(true)
	t.closeResult()
	if t.kind == chatKind {
		return t.putClosed.Load()
	}
	return true
}

// TryPutClose marks the client->server (streamOut) side finished. For a
// put-stream this is the terminal event and closes resultCh; for chat the close
// is owned by the get side, so here we only flip the flag. Returns true when
// the whole request is finished.
func (t *rpcRequestCtx) TryPutClose() bool {
	t.putClosed.Store(true)
	if t.kind == putStreamKind {
		t.closeResult()
		return true
	}
	return t.getClosed.Load()
}

func (t *rpcRequestCtx) SetError(err error) {
	t.resultErr.Store(&err)
}

func (t *rpcRequestCtx) Error(defaultError error) error {
	if e := t.resultErr.Load(); e != nil {
		return *e
	}
	return defaultError
}

// SingleResp waits for the first response, the timeout, or ctx cancellation,
// whichever comes first. onTimeout runs on either timeout or cancellation (it
// sends CancelRequest to the server). A ctx with no Done channel
// (context.Background) simply never selects that case.
func (t *rpcRequestCtx) SingleResp(ctx context.Context, timeoutMls int64, onTimeout func()) (value.Value, error) {
	// time.NewTimer + Stop instead of time.After: time.After cannot be cancelled
	// and its timer lives until it fires, leaking a timer per call on the unary
	// hot path when the response arrives first.
	timer := time.NewTimer(time.Duration(timeoutMls) * time.Millisecond)
	defer timer.Stop()
	select {
	case result, ok := <-t.resultCh:
		if !ok {
			return nil, t.Error(ErrNoResponse)
		}
		return result, nil
	case <-ctx.Done():
		onTimeout()
		return nil, ctx.Err()
	case <-timer.C:
		onTimeout()
		// The timer is set to the context's remaining deadline when that is the
		// shorter bound, so prefer the context error (DeadlineExceeded) for a
		// clear, idiomatic result; fall back to the plain timeout otherwise.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, t.Error(ErrTimeoutError)
	}
}

func (t *rpcRequestCtx) MultiResp() <-chan value.Value {
	return t.resultCh
}
