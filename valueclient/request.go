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

	resultCh  chan value.Value
	done      chan struct{} // closed together with resultCh
	closeOnce sync.Once

	getClosed atomic.Bool // no more server->client values will be delivered
	putClosed atomic.Bool // client->server side (streamOut) has finished

	resultErr        atomic.Pointer[error] // stdlib has no atomic.Error
	throttleOutgoing atomic.Int64
	throttleOnServer atomic.Int64
}

func NewRequestCtx(requestId int64, kind streamKind, req value.Map, receiveCap int) *rpcRequestCtx {
	return &rpcRequestCtx{
		requestId: requestId,
		kind:      kind,
		req:       req,
		start:     time.Now(),
		resultCh:  make(chan value.Value, receiveCap),
		done:      make(chan struct{}),
	}
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

// notifyResult delivers a server->client value. It never panics on a closed
// channel (BUG-6) and never blocks forever once the request is done; it still
// blocks while a live consumer is slow (backpressure, relieved by throttling).
func (t *rpcRequestCtx) notifyResult(res value.Value) {
	defer func() { _ = recover() }() // resultCh may close concurrently
	select {
	case t.resultCh <- res:
	case <-t.done:
	}
}

// closeResult closes resultCh (and done) exactly once.
func (t *rpcRequestCtx) closeResult() {
	t.closeOnce.Do(func() {
		close(t.done)
		close(t.resultCh)
	})
}

// Close force-closes the request: unary completion, error, or cancellation.
func (t *rpcRequestCtx) Close() {
	t.getClosed.Store(true)
	t.putClosed.Store(true)
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
	select {
	case result, ok := <-t.resultCh:
		if !ok {
			return nil, t.Error(ErrNoResponse)
		}
		return result, nil
	case <-time.After(time.Duration(timeoutMls) * time.Millisecond):
		onTimeout()
		return nil, t.Error(ErrTimeoutError)
	}
}

func (t *rpcRequestCtx) MultiResp() <-chan value.Value {
	return t.resultCh
}
