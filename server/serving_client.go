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

package server

import (
	"fmt"
	"github.com/consensusdb/value"
	"github.com/consensusdb/value-rpc/rpc"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"sync"
)

/**
@author Alex Shvid
*/

var OutgoingQueueCap = 4096

type servingClient struct {
	clientId    int64
	activeConn  atomic.Value
	functionMap *sync.Map

	logger *zap.Logger

	outgoingQueue chan value.Map

	requestMap sync.Map
}

func NewServingClient(clientId int64, conn rpc.MsgConn, functionMap *sync.Map, logger *zap.Logger) *servingClient {

	client := &servingClient{
		clientId:      clientId,
		functionMap:   functionMap,
		outgoingQueue: make(chan value.Map, OutgoingQueueCap),
		logger:        logger,
	}
	client.activeConn.Store(conn)

	go client.sender()

	return client
}

func (t *servingClient) Close() {

	t.requestMap.Range(func(key, value interface{}) bool {
		sr := value.(*servingRequest)
		sr.Close()
		return true
	})

	close(t.outgoingQueue)
}

func (t *servingClient) replaceConn(newConn rpc.MsgConn) {

	oldConn := t.activeConn.Load()
	if oldConn != nil {
		oldConn.(rpc.MsgConn).Close()
	}

	t.activeConn.Store(newConn)
	go t.sender()
}

func FunctionResult(requestId value.Number, result value.Value) value.Map {
	resp := value.EmptyMap().
		Put(rpc.MessageTypeField, rpc.FunctionResponse.Long()).
		Put(rpc.RequestIdField, requestId)
	if result != nil {
		return resp.Put(rpc.ResultField, result)
	} else {
		return resp
	}
}

func StreamReady(requestId value.Number) value.Map {
	return value.EmptyMap().
		Put(rpc.MessageTypeField, rpc.StreamReady.Long()).
		Put(rpc.RequestIdField, requestId)
}

func StreamValue(requestId value.Number, val value.Value) value.Map {
	return value.EmptyMap().
		Put(rpc.MessageTypeField, rpc.StreamValue.Long()).
		Put(rpc.RequestIdField, requestId).
		Put(rpc.ValueField, val)
}

func StreamEnd(requestId value.Number, val value.Value) value.Map {
	resp := value.EmptyMap().
		Put(rpc.MessageTypeField, rpc.StreamEnd.Long()).
		Put(rpc.RequestIdField, requestId)
	if val != nil {
		return resp.Put(rpc.ValueField, val)
	} else {
		return resp
	}
}

func FunctionError(requestId value.Number, format string, args ...interface{}) value.Map {
	resp := value.EmptyMap().
		Put(rpc.MessageTypeField, rpc.ErrorResponse.Long()).
		Put(rpc.RequestIdField, requestId)
	if len(args) == 0 {
		return resp.Put(rpc.ErrorField, value.Utf8(format))
	} else {
		s := fmt.Sprintf(format, args...)
		return resp.Put(rpc.ErrorField, value.Utf8(s))
	}
}

func (t *servingClient) sender() {

	for {

		resp, ok := <-t.outgoingQueue
		if !ok {
			t.logger.Info("stop serving client", zap.Int64("clientId", t.clientId))
			break
		}

		conn := t.activeConn.Load()
		if conn == nil {
			t.logger.Error("sender no active connection")
			break
		}

		msgConn := conn.(rpc.MsgConn)
		err := msgConn.WriteMessage(resp)

		if err != nil {
			// io error
			t.send(resp)
			t.logger.Error("sender write message", zap.Error(err))
			break
		}

	}
}

func (t *servingClient) send(resp value.Map) error {
	t.outgoingQueue <- resp
	return nil
}

func (t *servingClient) findFunction(name string) (*function, bool) {
	if fn, ok := t.functionMap.Load(name); ok {
		return fn.(*function), true
	}
	return nil, false
}

func (t *servingClient) serveFunctionRequest(ft functionType, req value.Map) {
	resp := t.doServeFunctionRequest(ft, req)
	if resp != nil {
		t.send(resp)
	}
}

func (t *servingClient) doServeFunctionRequest(ft functionType, req value.Map) value.Map {

	reqId := req.GetNumber(rpc.RequestIdField)
	if reqId == nil {
		return FunctionError(reqId, "request id not found")
	}

	name := req.GetString(rpc.FunctionNameField)
	if name == nil {
		return FunctionError(reqId, "function name field not found")
	}

	fn, ok := t.findFunction(name.String())
	if !ok {
		return FunctionError(reqId, "function not found %s", name.String())
	}

	args, _ := req.Get(rpc.ArgumentsField)
	if !Verify(args, fn.args) {
		return FunctionError(reqId, "function '%s' invalid args %s", name.String(), value.Jsonify(args))
	}

	if fn.ft != ft {
		return FunctionError(reqId, "function wrong type %s, expected %d, actual %d", name.String(), fn.ft, ft)
	}

	switch fn.ft {
	case singleFunction:
		res, err := fn.singleFn(args)
		if err != nil {
			return FunctionError(reqId, "single function %s call, %v", name.String(), err)
		}
		if !Verify(res, fn.res) {
			return FunctionError(reqId, "function '%s' invalid results %s", name.String(), value.Jsonify(res))
		}
		return FunctionResult(reqId, res)

	case outgoingStream:
		sr := t.newServingRequest(ft, reqId)
		outC, err := fn.outStream(args)
		if err != nil {
			sr.closeRequest(t)
			return FunctionError(reqId, "out stream function %s call, %v", name.String(), err)
		}
		go sr.outgoingStreamer(outC, t)
		return nil

	case incomingStream:
		sr := t.newServingRequest(ft, reqId)
		err := fn.inStream(args, sr.inC)
		if err != nil {
			sr.closeRequest(t)
			return FunctionError(reqId, "in stream function %s call, %v", name.String(), err)
		}
		return StreamReady(reqId)

	case chat:
		sr := t.newServingRequest(ft, reqId)
		outC, err := fn.chat(args, sr.inC)
		if err != nil {
			sr.closeRequest(t)
			return FunctionError(reqId, "chat function %s call, %v", name.String(), err)
		}
		go sr.outgoingStreamer(outC, t)
		return nil
	}

	return FunctionError(reqId, "unsupported function %s type", name.String())

}

func (t *servingClient) newServingRequest(ft functionType, reqId value.Number) *servingRequest {
	sr := NewServingRequest(ft, reqId)
	t.requestMap.Store(reqId.Long(), sr)
	return sr
}

func (t *servingClient) findServingRequest(reqId value.Number) (*servingRequest, bool) {

	requestCtx, ok := t.requestMap.Load(reqId.Long())
	if !ok {
		return nil, false
	}

	return requestCtx.(*servingRequest), true

}

func (t *servingClient) deleteRequest(requestId value.Number) {
	t.requestMap.Delete(requestId.Long())
}

func (t *servingClient) processRequest(req value.Map) error {
	//t.logger.Info("processRequest", zap.Stringer("req", req))

	mt := req.GetNumber(rpc.MessageTypeField)
	if mt == nil {
		return errors.Errorf("empty message type in %s", req.String())
	}

	reqId := req.GetNumber(rpc.RequestIdField)
	if reqId == nil {
		return errors.Errorf("request id not found in %s", req.String())
	}

	if sr, ok := t.findServingRequest(reqId); ok {
		return sr.serveRunningRequest(mt, req, t)
	} else {
		return t.serveNewRequest(mt, req)
	}

}

func (t *servingClient) serveNewRequest(mt value.Number, req value.Map) error {

	switch rpc.MessageType(mt.Long()) {

	case rpc.FunctionRequest:
		go t.serveFunctionRequest(singleFunction, req)

	case rpc.GetStreamRequest:
		go t.serveFunctionRequest(outgoingStream, req)

	case rpc.PutStreamRequest:
		go t.serveFunctionRequest(incomingStream, req)

	case rpc.ChatRequest:
		go t.serveFunctionRequest(chat, req)

	default:
		return errors.Errorf("unknown message type for new request in %s", req.String())
	}

	return nil
}
