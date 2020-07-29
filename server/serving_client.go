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
Alex Shvid
*/

var outgoingQueueCap = 4096

type servingClient struct {
	clientId    int64
	activeConn  atomic.Value
	functionMap *sync.Map

	logger *zap.Logger

	outgoingQueue chan value.Table
}

func NewServingClient(clientId int64, conn rpc.MsgConn, functionMap *sync.Map, logger *zap.Logger) *servingClient {

	client := &servingClient{
		clientId:      clientId,
		functionMap:   functionMap,
		outgoingQueue: make(chan value.Table, outgoingQueueCap),
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

func ValueResult(req value.Table, msgType rpc.MessageType, result value.Value) value.Table {
	resp := value.Map()
	resp.Put(rpc.MessageTypeField, msgType.Long())
	resp.Put(rpc.RequestIdField, req.Get(rpc.RequestIdField))
	if result != nil {
		resp.Put(rpc.ValueField, result)
	}
	return resp
}

func ServerError(req value.Table, msgType rpc.MessageType, msg string, args ...interface{}) value.Table {
	resp := value.Map()
	resp.Put(rpc.MessageTypeField, msgType.Long())
	resp.Put(rpc.RequestIdField, req.Get(rpc.RequestIdField))
	if len(args) == 0 {
		resp.Put(rpc.ErrorField, value.Utf8(msg))
	} else {
		s := fmt.Sprintf(msg, args...)
		resp.Put(rpc.ErrorField, value.Utf8(s))
	}
	return resp
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

func (t *servingClient) send(resp value.Table) error {
	t.outgoingQueue <- resp
	return nil
}

func (t *servingClient) findFunction(name string) (*function, bool) {
	if fn, ok := t.functionMap.Load(name); ok {
		return fn.(*function), true
	}
	return nil, false
}

func (t *servingClient) serveFunctionRequest(req value.Table) {
	t.send(t.doServeFunctionRequest(req))
}

func (t *servingClient) doServeFunctionRequest(req value.Table) value.Table {

	name := req.GetString(rpc.FunctionNameField)
	if name == nil {
		return ServerError(req, rpc.FunctionResponse, "function name field not found")
	}

	fn, ok := t.findFunction(name.String())
	if !ok {
		return ServerError(req, rpc.FunctionResponse, "function not found %s", name.String())
	}

	args := make([]value.Value, fn.args)
	for i := 0; i < fn.args; i++ {
		args[i] = req.GetAt(i)
	}

	res, err := fn.cb(args)
	if err != nil {
		return ServerError(req, rpc.FunctionResponse, "function %s error, %v", name.String(), err)
	}

	return ValueResult(req, rpc.FunctionResponse, res)
}

func (t *servingClient) serveGetStreamRequest(req value.Table) {

}

func (t *servingClient) servePutStreamRequest(req value.Table) {
}

func (t *servingClient) serveChatRequest(req value.Table) {

}
