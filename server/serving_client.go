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
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"sync"
)

/**
@author Alex Shvid
*/

var outgoingQueueCap = 4096

type servingClient struct {
	clientId    int64
	activeConn  atomic.Value
	functionMap *sync.Map

	logger *zap.Logger

	outgoingQueue chan value.Map
}

func NewServingClient(clientId int64, conn rpc.MsgConn, functionMap *sync.Map, logger *zap.Logger) *servingClient {

	client := &servingClient{
		clientId:      clientId,
		functionMap:   functionMap,
		outgoingQueue: make(chan value.Map, outgoingQueueCap),
		logger:        logger,
	}
	client.activeConn.Store(conn)

	go client.sender()

	return client
}

func (t *servingClient) Close() {
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

func FunctionResult(req value.Map, result value.Value) value.Map {
	resp := value.EmptyMap().
		Put(rpc.MessageTypeField, rpc.FunctionResponse.Long()).
		Put(rpc.RequestIdField, req.GetNumber(rpc.RequestIdField))
	if result != nil {
		return resp.Put(rpc.ResultField, result)
	} else {
		return resp
	}
}

func FunctionError(req value.Map, format string, args ...interface{}) value.Map {
	resp := value.EmptyMap().
		Put(rpc.MessageTypeField, rpc.FunctionResponse.Long()).
		Put(rpc.RequestIdField, req.GetNumber(rpc.RequestIdField))
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

func (t *servingClient) serveFunctionRequest(req value.Map) {
	t.send(t.doServeFunctionRequest(req))
}

func (t *servingClient) doServeFunctionRequest(req value.Map) value.Map {

	name := req.GetString(rpc.FunctionNameField)
	if name == nil {
		return FunctionError(req, "function name field not found")
	}

	fn, ok := t.findFunction(name.String())
	if !ok {
		return FunctionError(req, "function not found %s", name.String())
	}

	args := req.GetList(rpc.ArgumentsField)

	numArgs := 0
	if args != nil {
		numArgs = args.Len()
	}

	if fn.numArgs != numArgs {
		return FunctionError(req, "function wrong number of args %s, expected %d but actual %d", name.String(), fn.numArgs, numArgs)
	}

	var res value.Value
	var err error
	if numArgs > 0 {
		res, err = fn.cb(args.Values()...)
	} else {
		res, err = fn.cb()
	}

	if err != nil {
		return FunctionError(req, "function %s error, %v", name.String(), err)
	}

	return FunctionResult(req, res)
}

func (t *servingClient) serveGetStreamRequest(req value.Map) {

}

func (t *servingClient) servePutStreamRequest(req value.Map) {
}

func (t *servingClient) serveChatRequest(req value.Map) {

}
