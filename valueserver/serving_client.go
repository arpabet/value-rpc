/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
)

var OutgoingQueueCap = 4096

type servingClient struct {
	clientId    int64
	activeConn  atomic.Value
	functionMap *sync.Map

	logger *zap.Logger

	outgoingQueue chan value.Map
	done          chan struct{} // closed by Close(); never close outgoingQueue (BUG-3)

	requestMap       sync.Map
	canceledRequests sync.Map

	closeOnce sync.Once
}

func NewServingClient(clientId int64, conn vrpc.MsgConn, functionMap *sync.Map, logger *zap.Logger) *servingClient {

	client := &servingClient{
		clientId:      clientId,
		functionMap:   functionMap,
		outgoingQueue: make(chan value.Map, OutgoingQueueCap),
		done:          make(chan struct{}),
		logger:        logger,
	}
	client.activeConn.Store(conn)

	// Exactly one long-lived sender for the lifetime of the serving client; it
	// always writes to the current activeConn, so reconnects must not start
	// another one (BUG-8).
	go client.sender()

	return client
}

func (t *servingClient) Close() {

	t.closeOnce.Do(func() {
		// Signal the sender and any blocked producers to stop. We must NOT
		// close(outgoingQueue): producers (handlers, streamers) may still send
		// and would panic on a closed channel (BUG-3).
		close(t.done)

		// Unblock the connection read loop so it can exit.
		if c := t.activeConn.Load(); c != nil {
			c.(vrpc.MsgConn).Close()
		}

		t.requestMap.Range(func(key, value interface{}) bool {
			sr := value.(*servingRequest)
			sr.Close()
			return true
		})
	})

}

func (t *servingClient) replaceConn(newConn vrpc.MsgConn) {

	oldConn := t.activeConn.Load()
	if oldConn != nil {
		oldConn.(vrpc.MsgConn).Close()
	}

	t.activeConn.Store(newConn)
	// The single sender (started in NewServingClient) picks up the new conn via
	// activeConn; starting another sender here caused duplicates (BUG-8).
}

func FunctionResult(requestId value.Number, result value.Value) value.Map {
	resp := value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.FunctionResponse.Long()).
		Put(vrpc.RequestIdField, requestId)
	if result != nil {
		return resp.Put(vrpc.ResultField, result)
	} else {
		return resp
	}
}

func StreamReady(requestId value.Number) value.Map {
	return value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.StreamReady.Long()).
		Put(vrpc.RequestIdField, requestId)
}

func StreamValue(requestId value.Number, val value.Value) value.Map {
	return value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.StreamValue.Long()).
		Put(vrpc.RequestIdField, requestId).
		Put(vrpc.ValueField, val)
}

func StreamEnd(requestId value.Number, val value.Value) value.Map {
	resp := value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.StreamEnd.Long()).
		Put(vrpc.RequestIdField, requestId)
	if val != nil {
		return resp.Put(vrpc.ValueField, val)
	} else {
		return resp
	}
}

func FunctionError(requestId value.Number, format string, args ...interface{}) value.Map {
	resp := value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.ErrorResponse.Long()).
		Put(vrpc.RequestIdField, requestId)
	if len(args) == 0 {
		return resp.Put(vrpc.ErrorField, value.Utf8(format))
	} else {
		s := fmt.Sprintf(format, args...)
		return resp.Put(vrpc.ErrorField, value.Utf8(s))
	}
}

func (t *servingClient) sender() {

	for {

		select {
		case <-t.done:
			t.logger.Info("stop serving client", zap.Int64("clientId", t.clientId))
			return
		case resp, ok := <-t.outgoingQueue:
			if !ok {
				return
			}

			conn := t.activeConn.Load()
			if conn == nil {
				t.logger.Error("sender no active connection")
				continue
			}

			if err := conn.(vrpc.MsgConn).WriteMessage(resp); err != nil {
				// BUG-9 fix: do not re-enqueue (a full queue would deadlock) and
				// do not stop the only sender; the connection is replaced on
				// reconnect, after which sends resume.
				t.logger.Error("sender write message", zap.Error(err))
			}
		}

	}
}

func (t *servingClient) send(resp value.Map) error {
	select {
	case t.outgoingQueue <- resp:
		return nil
	case <-t.done:
		return vrpc.ErrClientClosed
	}
}

func (t *servingClient) findFunction(name string) (*function, bool) {
	if fn, ok := t.functionMap.Load(name); ok {
		return fn.(*function), true
	}
	return nil, false
}

func (t *servingClient) serveFunctionRequest(ft functionType, req value.Map) {
	// This runs in its own goroutine; a panicking user handler must not crash
	// the whole server process.
	defer func() {
		if r := recover(); r != nil {
			t.logger.Error("recovered in serveFunctionRequest", zap.Any("recover", r))
		}
	}()
	resp := t.doServeFunctionRequest(ft, req)
	if resp != nil {
		t.send(resp)
	}
}

func (t *servingClient) doServeFunctionRequest(ft functionType, req value.Map) value.Map {

	reqId, ok := vrpc.GetNumberField(req, vrpc.RequestIdField)
	if !ok {
		// Without a request id the response cannot be routed; reply with id 0.
		reqId = value.Long(0)
	}

	name, ok := vrpc.GetStringField(req, vrpc.FunctionNameField)
	if !ok {
		return FunctionError(reqId, "function name field not found")
	}

	fn, ok := t.findFunction(name.String())
	if !ok {
		return FunctionError(reqId, "function not found %s", name.String())
	}

	args := req.Get(vrpc.ArgumentsField)
	if !vrpc.Verify(args, fn.args) {
		return FunctionError(reqId, "function '%s' invalid args %s", name.String(), value.Jsonify(args))
	}

	if fn.ft != ft {
		return FunctionError(reqId, "function wrong type %s, expected %d, actual %d", name.String(), fn.ft, ft)
	}

	if _, ok := t.canceledRequests.Load(reqId.Long()); ok {
		t.canceledRequests.Delete(reqId.Long())
		return FunctionError(reqId, "function '%s' canceled request %d", name.String(), reqId.Long())
	}

	switch fn.ft {
	case singleFunction:
		res, err := fn.singleFn(args)
		if err != nil {
			return FunctionError(reqId, "single function %s call, %v", name.String(), err)
		}
		if !vrpc.Verify(res, fn.res) {
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

	mt, ok := vrpc.GetNumberField(req, vrpc.MessageTypeField)
	if !ok {
		return errors.Errorf("empty message type in %s", req.String())
	}
	msgType := vrpc.MessageType(mt.Long())

	reqId, ok := vrpc.GetNumberField(req, vrpc.RequestIdField)
	if !ok {
		return errors.Errorf("request id not found in %s", req.String())
	}

	if sr, ok := t.findServingRequest(reqId); ok {
		return sr.serveRunningRequest(msgType, req, t)
	} else {
		if msgType == vrpc.CancelRequest {
			t.canceledRequests.Store(reqId.Long(), req)
			return nil
		}
		return t.serveNewRequest(msgType, req)
	}

}

func (t *servingClient) serveNewRequest(msgType vrpc.MessageType, req value.Map) error {

	switch msgType {

	case vrpc.FunctionRequest:
		go t.serveFunctionRequest(singleFunction, req)

	case vrpc.GetStreamRequest:
		go t.serveFunctionRequest(outgoingStream, req)

	case vrpc.PutStreamRequest:
		go t.serveFunctionRequest(incomingStream, req)

	case vrpc.ChatRequest:
		go t.serveFunctionRequest(chat, req)

	default:
		return errors.Errorf("unknown message type for new request in %s", req.String())
	}

	return nil
}
