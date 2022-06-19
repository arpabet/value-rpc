/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
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
