/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"context"
	"fmt"

	"go.arpabet.com/value"
)

// Function is a unary handler: it receives the call arguments and returns a
// result (or an error carrying a Code). Both a server (for client->server calls)
// and a client (for server->client calls) register Functions, so the handler
// type lives here in the shared core rather than on one side of the connection.
type Function func(ctx context.Context, args value.Value) (value.Value, error)

// OutgoingStream, IncomingStream, and Chat are the streaming handler types. Like
// Function, they live in the shared core because either end can register them:
// the server serves streams the client opens, and (symmetrically) the client
// serves streams the server opens.
type OutgoingStream func(ctx context.Context, args value.Value) (<-chan value.Value, error)
type IncomingStream func(ctx context.Context, args value.Value, inC <-chan value.Value) error
type Chat func(ctx context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error)

// Caller is the established-connection surface for *initiating* a unary call to
// the peer at the other end of the connection. Both valueclient.Client and the
// server's per-connection handle implement it, so a call site reads the same
// whether it runs on the dialing side or the serving side.
type Caller interface {
	CallFunction(ctx context.Context, name string, args value.Value) (value.Value, error)
}

// Peer is the full symmetric surface of an established connection: unary plus
// the three streaming patterns. vRPC is peer-symmetric — which side dialed is a
// connection-setup detail, not a capability limit — so once connected, either
// end can initiate any pattern (and serve any pattern via the registrar Add*
// methods). Code written against Peer is identical on the client and the server.
type Peer interface {
	Caller
	GetStream(ctx context.Context, name string, args value.Value, receiveCap int) (<-chan value.Value, int64, error)
	PutStream(ctx context.Context, name string, args value.Value, putCh <-chan value.Value) error
	Chat(ctx context.Context, name string, args value.Value, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error)
}

// NewFunctionRequest builds a FunctionRequest envelope using dialect d. The
// caller assigns the request id; for a server->client call it must come from the
// server-initiated (negative) id space so it cannot collide with a client's own
// request ids on the same connection.
func (d *Dialect) NewFunctionRequest(requestId int64, name string, args value.Value) value.Map {
	req := value.EmptyMap(true).
		Put(d.MessageTypeField, FunctionRequest.Long()).
		Put(d.RequestIdField, value.Long(requestId)).
		Put(d.FunctionNameField, value.Utf8(name))
	if args != nil {
		req = req.Put(d.ArgumentsField, args)
	}
	return req
}

// NewFunctionRequest builds a FunctionRequest on the active DefaultDialect.
func NewFunctionRequest(requestId int64, name string, args value.Value) value.Map {
	return DefaultDialect.NewFunctionRequest(requestId, name, args)
}

// NewFunctionResult builds a FunctionResponse carrying result (omitted when nil,
// so a void result decodes as absent rather than a Null sentinel).
func NewFunctionResult(requestId value.Number, result value.Value) value.Map {
	resp := value.EmptyMap(true).
		Put(DefaultDialect.MessageTypeField, FunctionResponse.Long()).
		Put(DefaultDialect.RequestIdField, requestId)
	if result != nil {
		resp = resp.Put(DefaultDialect.ResultField, result)
	}
	return resp
}

// NewFunctionError builds an ErrorResponse carrying a machine-readable Code and
// a formatted message.
func NewFunctionError(requestId value.Number, code Code, format string, args ...interface{}) value.Map {
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	return value.EmptyMap(true).
		Put(DefaultDialect.MessageTypeField, ErrorResponse.Long()).
		Put(DefaultDialect.RequestIdField, requestId).
		Put(DefaultDialect.CodeField, value.Long(int64(code))).
		Put(DefaultDialect.ErrorField, value.Utf8(msg))
}

// NewStreamRequest builds a stream-opening request (GetStreamRequest /
// PutStreamRequest / ChatRequest) using dialect-default fields. The caller
// assigns the request id (server-initiated streams use the negative id space).
func NewStreamRequest(mt MessageType, requestId int64, name string, args value.Value) value.Map {
	req := value.EmptyMap(true).
		Put(DefaultDialect.MessageTypeField, mt.Long()).
		Put(DefaultDialect.RequestIdField, value.Long(requestId)).
		Put(DefaultDialect.FunctionNameField, value.Utf8(name))
	if args != nil {
		req = req.Put(DefaultDialect.ArgumentsField, args)
	}
	return req
}

// NewStreamReady builds the StreamReady ack a stream responder sends once its
// handler is established (before it acquires any send credit).
func NewStreamReady(requestId value.Number) value.Map {
	return value.EmptyMap(true).
		Put(DefaultDialect.MessageTypeField, StreamReady.Long()).
		Put(DefaultDialect.RequestIdField, requestId)
}

// NewStreamValue builds a StreamValue frame carrying one streamed value.
func NewStreamValue(requestId value.Number, val value.Value) value.Map {
	return value.EmptyMap(true).
		Put(DefaultDialect.MessageTypeField, StreamValue.Long()).
		Put(DefaultDialect.RequestIdField, requestId).
		Put(DefaultDialect.ValueField, val)
}

// NewStreamEnd builds a StreamEnd frame, optionally carrying a final value.
func NewStreamEnd(requestId value.Number, val value.Value) value.Map {
	resp := value.EmptyMap(true).
		Put(DefaultDialect.MessageTypeField, StreamEnd.Long()).
		Put(DefaultDialect.RequestIdField, requestId)
	if val != nil {
		resp = resp.Put(DefaultDialect.ValueField, val)
	}
	return resp
}

// HandlerErrorCode maps a user-handler error to a Code: the code of a
// *valuerpc.Error, the matching code for a context deadline/cancellation (which
// a handler typically returns as ctx.Err()), else Internal.
func HandlerErrorCode(err error) Code {
	if code := CodeOf(err); code != CodeUnknown {
		return code
	}
	switch err {
	case context.DeadlineExceeded:
		return CodeDeadlineExceeded
	case context.Canceled:
		return CodeCanceled
	default:
		return CodeInternal
	}
}

// NewHandlerError builds an ErrorResponse from an error a user handler returned,
// honouring its Code when it is a *valuerpc.Error and defaulting to Internal.
// For a coded error its plain message is used (not its String()) so the code
// prefix is not duplicated when the caller rebuilds a *valuerpc.Error.
func NewHandlerError(requestId value.Number, where string, err error) value.Map {
	msg := err.Error()
	if e, ok := err.(*Error); ok {
		msg = e.Message
	}
	return NewFunctionError(requestId, HandlerErrorCode(err), "%s: %s", where, msg)
}
