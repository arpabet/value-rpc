/*
 *
 * Copyright 2020-present Arpabet Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package valueclient

import (
	"github.com/consensusdb/value"
	"github.com/consensusdb/value-rpc/valuerpc"
	"go.uber.org/atomic"
	"time"
)

/**
@author Alex Shvid
*/

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
	st := t.state.Load()
	if st&getStreamFlag > 0 {
		close(t.resultCh)
	}
	t.state.Store(0)
}

func (t *rpcRequestCtx) Closed() bool {
	return t.state.Load() == 0
}

func (t *rpcRequestCtx) IsGetOpen() bool {
	st := t.state.Load()
	return st&getStreamFlag > 0
}

func (t *rpcRequestCtx) TryGetClose() bool {

	st := t.state.Load()
	if st&getStreamFlag > 0 {
		st -= getStreamFlag
		t.state.Store(st)
		close(t.resultCh)
	}

	return st == 0
}

func (t *rpcRequestCtx) IsPutOpen() bool {
	st := t.state.Load()
	return st&putStreamFlag > 0
}

func (t *rpcRequestCtx) TryPutClose() bool {

	st := t.state.Load()
	if st&putStreamFlag > 0 {
		st -= putStreamFlag
		t.state.Store(st)
	}
	return st == 0
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
