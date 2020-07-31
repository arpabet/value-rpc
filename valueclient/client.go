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
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

/**
@author Alex Shvid
*/

type responseHandler func(resp value.Map)

var DefaultSendingCap = int64(1024)
var DefaultTimeoutMls = int64(1000) // one second

type rpcClient struct {
	address           string
	socks5            string
	clientId          int64
	sendingCap        int64
	conn              *syncConn
	lastRequest       atomic.Int64
	reconnects        atomic.Int64
	requestCtxMap     sync.Map
	connectionHandler atomic.Value
	errorHandler      atomic.Value
	timeoutMls        atomic.Int64
	perfMonitor       atomic.Value
	shuttingDown      atomic.Bool
}

func NewClient(address, socks5 string) Client {

	t := &rpcClient{
		address:    address,
		socks5:     socks5,
		clientId:   rand.Int63(),
		sendingCap: DefaultSendingCap,
		conn:       NewSyncConn(),
	}

	t.timeoutMls.Store(DefaultTimeoutMls)
	return t
}

func (t *rpcClient) ClientId() int64 {
	return t.clientId
}

func (t *rpcClient) Stats() map[string]int64 {

	sendingLen, sendingCap := 0, 0
	if t.conn.hasConn() {
		sendingLen, sendingCap = t.conn.getConn().Stats()
	}

	return map[string]int64{
		"requests":   t.lastRequest.Load(),
		"reconnects": t.reconnects.Load(),
		"sendingLen": int64(sendingLen),
		"sendingCap": int64(sendingCap),
	}
}

func (t *rpcClient) Close() error {
	t.errorHandler.Store(t)
	t.shuttingDown.Store(true)
	t.conn.reset()
	return nil
}

func (t *rpcClient) getConnectionHandler() ConnectionHandler {
	ch := t.connectionHandler.Load()
	if ch != nil {
		return ch.(ConnectionHandler)
	}
	return func(resp value.Map) {
		log.Println("New connection established with ", resp)
	}
}

func (t *rpcClient) SetConnectionHandler(ch ConnectionHandler) {
	t.connectionHandler.Store(ch)
}

func (t *rpcClient) getErrorHandler() ErrorHandler {
	eh := t.errorHandler.Load()
	if eh != nil {
		return eh.(ErrorHandler)
	}
	return t
}

func (t *rpcClient) SetErrorHandler(eh ErrorHandler) {
	t.errorHandler.Store(eh)
}

func (t *rpcClient) SetMonitor(perfMonitor PerformanceMonitor) {
	t.perfMonitor.Store(perfMonitor)
}

func (t *rpcClient) SetTimeout(timeoutMls int64) {
	t.timeoutMls.Store(timeoutMls)
}

func (t *rpcClient) BadConnection(err error) {

	if t.shuttingDown.Load() {
		return
	}

	log.Printf("ERROR: bad connection, reconnect, %v\n", err)
	err = t.Reconnect()
	if err != nil {
		log.Printf("ERROR: reconnect failed, %v\n", err)
	}
}

func (t *rpcClient) ProtocolError(rest value.Map, err error) {
	log.Printf("ERROR: wrong message received, %v\n", err)
	var out strings.Builder
	rest.PrintJSON(&out)
	log.Println(out.String())
}

func (t *rpcClient) StreamError(requestId int64, err error) {
	log.Printf("ERROR: in-stream error for request %d, %v\n", requestId, err)
}

func (t *rpcClient) IsActive() bool {
	return t.conn.hasConn()
}

func (t *rpcClient) Connect() error {
	if t.conn.hasConn() {
		return nil
	}
	return t.conn.connect(t.address, t.socks5, t.clientId, t.sendingCap, t.getResponseHandler(), t.getErrorHandler())
}

func (t *rpcClient) Reconnect() error {
	t.conn.reset()
	return t.Connect()
}

func (t *rpcClient) sendMetrics(requestCtx *rpcRequestCtx) {
	mon := t.perfMonitor.Load()
	if mon != nil {
		mon.(PerformanceMonitor)(requestCtx.Name(), requestCtx.Elapsed())
	}
}

func (t *rpcClient) processResponse(mt valuerpc.MessageType, resp value.Map, requestCtx *rpcRequestCtx) {

	switch mt {

	case valuerpc.FunctionResponse:
		result, _ := resp.Get(valuerpc.ResultField)
		requestCtx.notifyResult(result)
		t.sendMetrics(requestCtx)
		requestCtx.Close()
		t.requestCtxMap.Delete(requestCtx.requestId)

	case valuerpc.ErrorResponse:
		err := resp.GetString(valuerpc.ErrorField)
		serverErr := errors.Errorf("SERVER_FUNC_ERROR %v", err)
		requestCtx.SetError(serverErr)
		t.getErrorHandler().StreamError(requestCtx.requestId, serverErr)
		requestCtx.Close()
		t.requestCtxMap.Delete(requestCtx.requestId)

	case valuerpc.StreamReady:
		requestCtx.notifyResult(nil)

	case valuerpc.StreamValue:
		value, _ := resp.Get(valuerpc.ValueField)
		requestCtx.notifyResult(value)
		t.regulateIncomingStream(requestCtx)

	case valuerpc.StreamEnd:
		value, _ := resp.Get(valuerpc.ValueField)
		if value != nil {
			requestCtx.notifyResult(value)
		}
		if requestCtx.TryGetClose() {
			t.requestCtxMap.Delete(requestCtx.requestId)
		}

	case valuerpc.CancelRequest:
		if requestCtx.TryPutClose() {
			t.requestCtxMap.Delete(requestCtx.requestId)
		}

	case valuerpc.ThrottleIncrease:
		requestCtx.throttleOutgoing.Inc()

	case valuerpc.ThrottleDecrease:
		requestCtx.throttleOutgoing.Dec()

	default:
		t.getErrorHandler().ProtocolError(resp, ErrUnsupportedMessageType)

	}

}

func (t *rpcClient) regulateIncomingStream(requestCtx *rpcRequestCtx) {
	used, cap := requestCtx.Stats()
	if used*3 > cap {
		t.sendSystemRequest(requestCtx.requestId, valuerpc.ThrottleIncrease)
		requestCtx.throttleOnServer.Inc()
	} else if used == 0 && requestCtx.throttleOnServer.Load() > 0 {
		t.sendSystemRequest(requestCtx.requestId, valuerpc.ThrottleDecrease)
		requestCtx.throttleOnServer.Dec()
	}
}

func (t *rpcClient) getResponseHandler() responseHandler {
	return func(resp value.Map) {

		mt := resp.GetNumber(valuerpc.MessageTypeField)
		if mt == nil {
			t.getErrorHandler().ProtocolError(resp, ErrNoMessageType)
			return
		}
		msgType := valuerpc.MessageType(mt.Long())

		if msgType == valuerpc.HandshakeResponse {
			t.getConnectionHandler()(resp)
			return
		}

		id := resp.GetNumber(valuerpc.RequestIdField)
		if id == nil {
			t.getErrorHandler().ProtocolError(resp, ErrIdFieldNotFound)
			return
		}

		if entry, ok := t.requestCtxMap.Load(id.Long()); ok {
			requestCtx := entry.(*rpcRequestCtx)
			t.processResponse(msgType, resp, requestCtx)
		} else {
			t.getErrorHandler().ProtocolError(resp, ErrRequestNotFound)
		}
	}
}

func (t *rpcClient) newRequestCtx(requestId int64, req value.Map, receiveCap int) *rpcRequestCtx {
	requestCtx := NewRequestCtx(requestId, req, receiveCap)
	t.requestCtxMap.Store(requestId, requestCtx)
	return requestCtx
}

func (t *rpcClient) ensureConnection() error {

	if !t.conn.hasConn() {
		return t.Connect()
	}

	return nil
}

func (t *rpcClient) sendRequest(req value.Map, receiveCap int) (*rpcRequestCtx, error) {

	err := t.ensureConnection()
	if err != nil {
		return nil, err
	}

	requestId := t.lastRequest.Inc()
	req = req.Put(valuerpc.RequestIdField, value.Long(requestId))

	requestCtx := t.newRequestCtx(requestId, req, receiveCap)

	t.conn.getConn().SendRequest(req)
	return requestCtx, nil

}

func (t *rpcClient) sendSystemRequest(requestId int64, mt valuerpc.MessageType) {

	err := t.ensureConnection()
	if err != nil {
		return
	}

	req := value.EmptyMap().
		Put(valuerpc.MessageTypeField, mt.Long()).
		Put(valuerpc.RequestIdField, value.Long(requestId))

	t.conn.getConn().SendRequest(req)
}

func (t *rpcClient) CancelRequest(requestId int64) {
	t.sendSystemRequest(requestId, valuerpc.CancelRequest)
}

func (t *rpcClient) CallFunction(name string, args value.Value) (value.Value, error) {

	req := t.constructRequest(valuerpc.FunctionRequest, name, args, t.timeoutMls.Load())

	requestCtx, err := t.sendRequest(req, 1)
	if err != nil {
		return nil, err
	}

	res, err := requestCtx.SingleResp(t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.Close()
		return nil, err
	}

	return res, err
}

func (t *rpcClient) GetStream(name string, args value.Value, receiveCap int) (<-chan value.Value, int64, error) {

	req := t.constructRequest(valuerpc.GetStreamRequest, name, args, t.timeoutMls.Load())

	requestCtx, err := t.sendRequest(req, receiveCap)
	if err != nil {
		return nil, 0, err
	}

	_, err = requestCtx.SingleResp(t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.Close()
		return nil, 0, err
	}

	return requestCtx.MultiResp(), requestCtx.requestId, err
}

func (t *rpcClient) PutStream(name string, args value.Value, putCh <-chan value.Value) error {

	req := t.constructRequest(valuerpc.PutStreamRequest, name, args, t.timeoutMls.Load())

	requestCtx, err := t.sendRequest(req, 1)
	if err != nil {
		return err
	}

	_, err = requestCtx.SingleResp(t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.Close()
		return err
	}

	go t.streamOut(requestCtx, putCh)

	return nil
}

func (t *rpcClient) Chat(name string, args value.Value, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error) {

	req := t.constructRequest(valuerpc.ChatRequest, name, args, t.timeoutMls.Load())

	requestCtx, err := t.sendRequest(req, receiveCap+1)
	if err != nil {
		return nil, 0, err
	}

	_, err = requestCtx.SingleResp(t.timeoutMls.Load(), func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		requestCtx.Close()
		return nil, 0, err
	}

	go t.streamOut(requestCtx, putCh)

	return requestCtx.MultiResp(), requestCtx.requestId, nil
}

func (t *rpcClient) streamOut(requestCtx *rpcRequestCtx, putCh <-chan value.Value) {

	for requestCtx.IsPutOpen() {

		val, ok := <-putCh
		if !ok {
			endReq := value.EmptyMap().
				Put(valuerpc.MessageTypeField, valuerpc.StreamEnd.Long()).
				Put(valuerpc.RequestIdField, value.Long(requestCtx.requestId))
			t.conn.getConn().SendRequest(endReq)
			break
		}

		nextReq := value.EmptyMap().
			Put(valuerpc.MessageTypeField, valuerpc.StreamValue.Long()).
			Put(valuerpc.RequestIdField, value.Long(requestCtx.requestId)).
			Put(valuerpc.ValueField, val)

		t.conn.getConn().SendRequest(nextReq)

		th := requestCtx.throttleOutgoing.Load()
		if th > 0 {
			time.Sleep(time.Millisecond * time.Duration(th))
		}

	}

	if requestCtx.TryPutClose() {
		t.requestCtxMap.Delete(requestCtx.requestId)
	}

}

func (t *rpcClient) constructRequest(mt valuerpc.MessageType, name string, args value.Value, timeout int64) value.Map {

	req := value.EmptyMap().
		Put(valuerpc.MessageTypeField, mt.Long()).
		Put(valuerpc.FunctionNameField, value.Utf8(name)).
		Put(valuerpc.ArgumentsField, args)

	if timeout > 0 {
		req = req.Put(valuerpc.TimeoutField, value.Long(timeout))
	}

	return req
}
