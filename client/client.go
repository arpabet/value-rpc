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

package client

import (
	"github.com/consensusdb/value"
	"github.com/consensusdb/value-rpc/rpc"
	"go.uber.org/atomic"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

/**
Alex Shvid
*/

type responseHandler func(resp value.Table)

var DefaultSendingCap = int64(1024)

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
	perfMonitor       atomic.Value
	shuttingDown      atomic.Bool
}

func NewClient(address, socks5 string) Client {

	return &rpcClient{
		address:    address,
		socks5:     socks5,
		clientId:   rand.Int63(),
		sendingCap: DefaultSendingCap,
		conn:       NewSyncConn(),
	}

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
	return func(resp value.Table) {
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

func (t *rpcClient) ProtocolError(rest value.Table, err error) {
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

func (t *rpcClient) processResponse(mt rpc.MessageType, resp value.Table, requestCtx *rpcRequestCtx) {

	result, serverErr := rpc.ServerResult(resp)

	switch mt {

	case rpc.FunctionResponse:
		if serverErr != nil {
			requestCtx.SetError(serverErr)
		} else {
			requestCtx.notifyResult(result)
		}
		t.sendMetrics(requestCtx)
		requestCtx.Close()
		t.requestCtxMap.Delete(requestCtx.requestId)

	case rpc.StreamValue:
		if serverErr != nil {
			t.getErrorHandler().StreamError(requestCtx.requestId, serverErr)
		} else {
			requestCtx.notifyResult(result)
		}
		t.throttleStream(requestCtx)

	case rpc.StreamEnd:
		if serverErr != nil {
			t.getErrorHandler().StreamError(requestCtx.requestId, serverErr)
		} else if result != nil { // optional result on StreamEnd like in gRPC
			requestCtx.notifyResult(result)
		}
		if requestCtx.TryGetClose() {
			t.requestCtxMap.Delete(requestCtx.requestId)
		}

	case rpc.CancelRequest:
		if requestCtx.TryPutClose() {
			t.requestCtxMap.Delete(requestCtx.requestId)
		}

	default:
		if serverErr != nil {
			t.getErrorHandler().StreamError(requestCtx.requestId, serverErr)
		} else {
			requestCtx.notifyResult(result)
		}

	}

}

func (t *rpcClient) throttleStream(requestCtx *rpcRequestCtx) {
	used, cap := requestCtx.Stats()
	if used*3 > cap {
		t.sendSystemRequest(requestCtx.requestId, rpc.ThrottleIncrease)
		requestCtx.throttle.Inc()
	} else if used == 0 && requestCtx.throttle.Load() > 0 {
		t.sendSystemRequest(requestCtx.requestId, rpc.ThrottleDecrease)
		requestCtx.throttle.Dec()
	}
}

func (t *rpcClient) getResponseHandler() responseHandler {
	return func(resp value.Table) {

		mt := resp.GetNumber(rpc.MessageTypeField)
		if mt == nil {
			t.getErrorHandler().ProtocolError(resp, ErrNoMessageType)
			return
		}
		msgType := rpc.MessageType(mt.Long())

		if msgType == rpc.HandshakeResponse {
			t.getConnectionHandler()(resp)
			return
		}

		id := resp.GetNumber(rpc.RequestIdField)
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

func (t *rpcClient) newRequestCtx(req value.Table, receiveCap int) *rpcRequestCtx {
	requestId := t.lastRequest.Inc()
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

func (t *rpcClient) sendRequest(req value.Table, receiveCap int) (*rpcRequestCtx, error) {

	err := t.ensureConnection()
	if err != nil {
		return nil, err
	}

	requestCtx := t.newRequestCtx(req, receiveCap)
	req.Put(rpc.RequestIdField, value.Long(requestCtx.requestId))

	t.conn.getConn().SendRequest(req)
	return requestCtx, nil

}

func (t *rpcClient) sendSystemRequest(requestId int64, mt rpc.MessageType) {

	err := t.ensureConnection()
	if err != nil {
		return
	}

	req := value.Map()
	req.Put(rpc.MessageTypeField, mt.Long())
	req.Put(rpc.RequestIdField, value.Long(requestId))
	t.conn.getConn().SendRequest(req)
}

func (t *rpcClient) CancelRequest(requestId int64) {
	t.sendSystemRequest(requestId, rpc.CancelRequest)
}

func (t *rpcClient) CallFunction(name string, args []value.Value, timeout time.Duration) (value.Value, error) {

	req := t.constructRequest(rpc.FunctionRequest, name, args, timeout.Milliseconds())

	requestCtx, err := t.sendRequest(req, 1)
	if err != nil {
		return nil, err
	}

	res, err := requestCtx.SingleResp(timeout, func() {
		t.CancelRequest(requestCtx.requestId)
	})
	return res, err
}

func (t *rpcClient) GetStream(name string, args []value.Value, receiveCap int) (<-chan value.Value, int64, error) {

	req := t.constructRequest(rpc.GetStreamRequest, name, args, 0)

	requestCtx, err := t.sendRequest(req, receiveCap)
	if err != nil {
		return nil, 0, err
	}

	return requestCtx.MultiResp(), requestCtx.requestId, nil
}

func (t *rpcClient) PutStream(name string, args []value.Value, timeout time.Duration, putCh <-chan value.Value) error {

	req := t.constructRequest(rpc.PutStreamRequest, name, args, timeout.Milliseconds())

	requestCtx, err := t.sendRequest(req, 1)
	if err != nil {
		return err
	}

	// get acknowledgement
	_, err = requestCtx.SingleResp(timeout, func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		return err
	}

	go t.streamOut(req, requestCtx, putCh)

	return nil
}

func (t *rpcClient) Chat(name string, args []value.Value, timeout time.Duration, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error) {

	req := t.constructRequest(rpc.ChatRequest, name, args, timeout.Milliseconds())

	requestCtx, err := t.sendRequest(req, receiveCap+1)
	if err != nil {
		return nil, 0, err
	}

	// get acknowledgement
	_, err = requestCtx.SingleResp(timeout, func() {
		t.CancelRequest(requestCtx.requestId)
	})
	if err != nil {
		return nil, 0, err
	}

	go t.streamOut(req, requestCtx, putCh)

	return requestCtx.MultiResp(), requestCtx.requestId, nil
}

func (t *rpcClient) streamOut(req value.Table, requestCtx *rpcRequestCtx, putCh <-chan value.Value) {

	for requestCtx.IsPutOpen() {

		val, ok := <-putCh
		if !ok {
			endReq := value.Map()
			endReq.Put(rpc.MessageTypeField, rpc.StreamEnd.Long())
			endReq.Put(rpc.RequestIdField, req.GetNumber(rpc.RequestIdField))
			t.conn.getConn().SendRequest(endReq)
			break
		}

		nextReq := value.Map()
		nextReq.Put(rpc.MessageTypeField, rpc.StreamValue.Long())
		nextReq.Put(rpc.RequestIdField, req.GetNumber(rpc.RequestIdField))
		nextReq.Put(rpc.ValueField, val)
		t.conn.getConn().SendRequest(nextReq)

	}

	if requestCtx.TryPutClose() {
		t.requestCtxMap.Delete(requestCtx.requestId)
	}

}

func (t *rpcClient) constructRequest(mt rpc.MessageType, name string, args []value.Value, timeout int64) value.Table {

	req := value.Map()
	req.Put(rpc.MessageTypeField, mt.Long())
	req.Put(rpc.FunctionNameField, value.Utf8(name))
	if timeout > 0 {
		req.Put(rpc.TimeoutField, value.Long(timeout))
	}

	for i, arg := range args {
		req.PutAt(i, arg)
	}

	return req
}
