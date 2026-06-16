/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"crypto/tls"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

type responseHandler func(resp value.Map)

var DefaultSendingCap = int64(1024)
var DefaultTimeoutMls = int64(1000) // one second

type rpcClient struct {
	dialer            valuerpc.Dialer
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

// NewClient creates a client for address. A bare "host:port" dials TCP; a scheme
// selects the transport: "tcp://host:port" or "unix:///path/to.sock". A non-empty
// socks5 (TCP only) routes through a SOCKS5 proxy. For full control use
// NewClientWithDialer.
func NewClient(address, socks5 string) Client {
	return NewClientWithDialer(valuerpc.NewDialer(address, socks5, KeepAlivePeriod, DefaultTimeout))
}

// NewUnixClient creates a client that dials the Unix-domain socket at path.
func NewUnixClient(path string) Client {
	return NewClientWithDialer(valuerpc.NewStreamDialer("unix", path, "", 0, DefaultTimeout))
}

// NewWebSocketClient creates a client that dials a WebSocket URL, e.g.
// "ws://host:9000/rpc" or "wss://host/rpc".
func NewWebSocketClient(url string) Client {
	return NewClientWithDialer(valuerpc.NewDialer(url, "", KeepAlivePeriod, DefaultTimeout))
}

// NewTLSClient creates a client that dials a TLS server over TCP. A nil config
// verifies against the system root CAs (server name derived from the address);
// supply a config for custom CAs, a client certificate (mTLS), or test options.
func NewTLSClient(address string, config *tls.Config) Client {
	return NewClientWithDialer(valuerpc.NewTLSDialer(address, config, KeepAlivePeriod, DefaultTimeout))
}

// NewMemClient creates a client that connects to an in-process server registered
// under name (see valueserver.NewMemServer). Same-process only.
func NewMemClient(name string) Client {
	return NewClientWithDialer(valuerpc.NewMemDialer(name))
}

// NewClientWithDialer creates a client over any transport (TCP, Unix socket,
// WebSocket, …) supplied as a valuerpc.Dialer.
func NewClientWithDialer(dialer valuerpc.Dialer) Client {

	t := &rpcClient{
		dialer:     dialer,
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
	return t.conn.connect(t.dialer, t.clientId, t.sendingCap, t.getResponseHandler(), t.getErrorHandler())
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
		// BUG-4/5 fix: an absent result field decodes as value.Null, not Go nil.
		// A void result must surface to the caller as nil, not a Null sentinel.
		result := resp.Get(valuerpc.ResultField)
		if result != nil && result.Kind() == value.NULL {
			result = nil
		}
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
		// BUG-4 fix: an absent value field decodes as value.Null, not Go nil;
		// only deliver a real payload.
		if streamValue := resp.Get(valuerpc.ValueField); streamValue != nil && streamValue.Kind() != value.NULL {
			requestCtx.notifyResult(streamValue)
		}
		t.regulateIncomingStream(requestCtx)

	case valuerpc.StreamEnd:
		// BUG-4 fix: do not deliver a phantom value.Null at end of stream.
		if streamEndValue := resp.Get(valuerpc.ValueField); streamEndValue != nil && streamEndValue.Kind() != value.NULL {
			requestCtx.notifyResult(streamEndValue)
		}
		if requestCtx.TryGetClose() {
			t.requestCtxMap.Delete(requestCtx.requestId)
		}

	case valuerpc.CancelRequest:
		requestCtx.Close()
		t.requestCtxMap.Delete(requestCtx.requestId)

	case valuerpc.ThrottleIncrease:
		requestCtx.throttleOutgoing.Add(1)

	case valuerpc.ThrottleDecrease:
		requestCtx.throttleOutgoing.Add(-1)

	default:
		t.getErrorHandler().ProtocolError(resp, ErrUnsupportedMessageType)

	}

}

func (t *rpcClient) regulateIncomingStream(requestCtx *rpcRequestCtx) {
	used, cap := requestCtx.Stats()
	if used*3 > cap {
		t.sendSystemRequest(requestCtx.requestId, valuerpc.ThrottleIncrease)
		requestCtx.throttleOnServer.Add(1)
	} else if used == 0 && requestCtx.throttleOnServer.Load() > 0 {
		t.sendSystemRequest(requestCtx.requestId, valuerpc.ThrottleDecrease)
		requestCtx.throttleOnServer.Add(-1)
	}
}

func (t *rpcClient) getResponseHandler() responseHandler {
	return func(resp value.Map) {

		// BUG-5 fix: GetNumber returns value.Zero (never nil) for a missing key,
		// so presence must be checked with GetNumberField, otherwise a message
		// with no type is silently treated as MessageType(0) = HandshakeResponse.
		mt, ok := valuerpc.GetNumberField(resp, valuerpc.MessageTypeField)
		if !ok {
			t.getErrorHandler().ProtocolError(resp, ErrNoMessageType)
			return
		}
		msgType := valuerpc.MessageType(mt.Long())

		if msgType == valuerpc.HandshakeResponse {
			t.getConnectionHandler()(resp)
			return
		}

		id, ok := valuerpc.GetNumberField(resp, valuerpc.RequestIdField)
		if !ok {
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

func (t *rpcClient) newRequestCtx(requestId int64, kind streamKind, req value.Map, receiveCap int) *rpcRequestCtx {
	requestCtx := NewRequestCtx(requestId, kind, req, receiveCap)
	t.requestCtxMap.Store(requestId, requestCtx)
	return requestCtx
}

func (t *rpcClient) ensureConnection() error {

	if !t.conn.hasConn() {
		return t.Connect()
	}

	return nil
}

func (t *rpcClient) sendRequest(kind streamKind, req value.Map, receiveCap int) (*rpcRequestCtx, error) {

	err := t.ensureConnection()
	if err != nil {
		return nil, err
	}

	requestId := t.lastRequest.Add(1)
	req = req.Put(valuerpc.RequestIdField, value.Long(requestId))

	requestCtx := t.newRequestCtx(requestId, kind, req, receiveCap)

	t.conn.getConn().SendRequest(req)
	return requestCtx, nil

}

func (t *rpcClient) sendSystemRequest(requestId int64, mt valuerpc.MessageType) {

	err := t.ensureConnection()
	if err != nil {
		return
	}

	req := value.EmptyMap(true).
		Put(valuerpc.MessageTypeField, mt.Long()).
		Put(valuerpc.RequestIdField, value.Long(requestId))

	t.conn.getConn().SendRequest(req)
}

func (t *rpcClient) CancelRequest(requestId int64) {
	t.sendSystemRequest(requestId, valuerpc.CancelRequest)
}

func (t *rpcClient) CallFunction(name string, args value.Value) (value.Value, error) {

	req := t.constructRequest(valuerpc.FunctionRequest, name, args, t.timeoutMls.Load())

	requestCtx, err := t.sendRequest(unaryKind, req, 1)
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

	requestCtx, err := t.sendRequest(getStreamKind, req, receiveCap)
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

	requestCtx, err := t.sendRequest(putStreamKind, req, 1)
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

	requestCtx, err := t.sendRequest(chatKind, req, receiveCap+1)
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
			endReq := value.EmptyMap(true).
				Put(valuerpc.MessageTypeField, valuerpc.StreamEnd.Long()).
				Put(valuerpc.RequestIdField, value.Long(requestCtx.requestId))
			t.conn.getConn().SendRequest(endReq)
			break
		}

		nextReq := value.EmptyMap(true).
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

	req := value.EmptyMap(true).
		Put(valuerpc.MessageTypeField, mt.Long()).
		Put(valuerpc.FunctionNameField, value.Utf8(name)).
		Put(valuerpc.ArgumentsField, args)

	if timeout > 0 {
		req = req.Put(valuerpc.TimeoutField, value.Long(timeout))
	}

	return req
}
