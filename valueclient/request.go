/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
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
	done       chan struct{}        // closed together with resultCh
	closeOnce  sync.Once
	cancelOnce sync.Once // bounds a slow-consumer cancel to one per request

	getClosed atomic.Bool // no more server->client values will be delivered
	putClosed atomic.Bool // client->server side (streamOut) has finished

	resultErr        atomic.Pointer[error] // stdlib has no atomic.Error
	throttleOutgoing atomic.Int64
	throttleOnServer atomic.Int64
}

func NewRequestCtx(requestId int64, kind streamKind, req value.Map, receiveCap int) *rpcRequestCtx {
	t := &rpcRequestCtx{
		requestId: requestId,
		kind:      kind,
		req:       req,
		start:     time.Now(),
		resultCh:  make(chan value.Value, receiveCap),
		done:      make(chan struct{}),
	}
	// Server->client streams (get-stream, chat) deliver through a pump so a slow
	// consumer cannot block the single response loop and every other request on
	// the connection (BUG-6). Unary and put-stream receive a single value into a
	// cap>=1 buffer and never head-of-line block, so they keep the direct path.
	if kind == getStreamKind || kind == chatKind {
		t.resultPump = valuerpc.NewStreamPump(t.resultCh, 0)
	}
	return t
}

func (t *rpcRequestCtx) Name() string {
	fn := t.req.GetString(valuerpc.FunctionNameField)
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
		if t.resultPump != nil {
			t.resultPump.Close() // drain, then close resultCh
		} else {
			close(t.resultCh)
		}
	})
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
	t.closeResult()
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

func (t *rpcRequestCtx) SingleResp(timeoutMls int64, onTimeout func()) (value.Value, error) {
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
	case <-timer.C:
		onTimeout()
		return nil, t.Error(ErrTimeoutError)
	}
}

func (t *rpcRequestCtx) MultiResp() <-chan value.Value {
	return t.resultCh
}
