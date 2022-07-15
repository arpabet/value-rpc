/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package valueclient

import (
	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/atomic"
	"time"
)


const getStreamFlag = 1
const putStreamFlag = 2

type rpcRequestCtx struct {
	requestId        int64
	state            atomic.Int32
	req              value.Map
	start            time.Time
	resultCh         chan value.Value
	resultErr        atomic.Error
	throttleOutgoing atomic.Int64
	throttleOnServer atomic.Int64
}

func NewRequestCtx(requestId int64, req value.Map, receiveCap int) *rpcRequestCtx {
	t := &rpcRequestCtx{
		requestId: requestId,
		req:       req,
		start:     time.Now(),
		resultCh:  make(chan value.Value, receiveCap),
	}
	t.state.Store(getStreamFlag + putStreamFlag)
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

func (t *rpcRequestCtx) notifyResult(res value.Value) {
	if t.IsGetOpen() {
		t.resultCh <- res
	}
}

func (t *rpcRequestCtx) Close() {
	doClose := false

	for {
		st := t.state.Load()
		if st & getStreamFlag > 0 {
			if t.state.CAS(st, 0) {
				doClose = true
				break
			}
		} else {
			break
		}
	}

	if doClose {
		close(t.resultCh)
	}

}

func (t *rpcRequestCtx) IsGetOpen() bool {
	st := t.state.Load()
	return st&getStreamFlag > 0
}

func (t *rpcRequestCtx) TryGetClose() bool {

	closed := false
	for {
		st := t.state.Load()
		if st & getStreamFlag > 0 {
			if t.state.CAS(st, st - getStreamFlag) {
				close(t.resultCh)
				closed = true
				break
			}
		} else {
			closed = true
			break
		}
	}

	return closed
}

func (t *rpcRequestCtx) IsPutOpen() bool {
	st := t.state.Load()
	return st&putStreamFlag > 0
}

func (t *rpcRequestCtx) TryPutClose() bool {

	closed := false
	for {
		st := t.state.Load()
		if st & putStreamFlag > 0 {
			if t.state.CAS(st, st - putStreamFlag) {
				close(t.resultCh)
				closed = true
				break
			}
		} else {
			closed = true
			break
		}
	}

	return closed
}

func (t *rpcRequestCtx) SetError(err error) {
	t.resultErr.Store(err)
}

func (t *rpcRequestCtx) Error(defaultError error) error {
	e := t.resultErr.Load()
	if e != nil {
		return e
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
