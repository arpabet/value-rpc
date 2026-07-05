/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"sync"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"golang.org/x/xerrors"
)

// DefaultTimeout bounds each message write on the connection.
var DefaultTimeout = 10 * time.Second

// KeepAlivePeriod enables TCP keepalive so dead peers are detected and their
// goroutines/fds reclaimed without killing idle-but-healthy streams (BUG-10).
// Ignored for non-TCP transports (e.g. Unix sockets).
var KeepAlivePeriod = 15 * time.Second

type rpcConn struct {
	conn         valuerpc.MsgConn
	reqCh        chan value.Map
	respHandler  responseHandler
	errorHandler ErrorHandler
	done         chan struct{}
	closeOnce    sync.Once
}

func newConn(ctx context.Context, dialer valuerpc.Dialer, clientId int64, resumeToken string, credential value.Value, sendingCap int64, respHandler responseHandler, errorHandler ErrorHandler) (*rpcConn, error) {

	conn, err := dialer.Dial(ctx)
	if err != nil {
		return nil, err
	}

	t := &rpcConn{
		conn:         conn,
		reqCh:        make(chan value.Map, sendingCap),
		respHandler:  respHandler,
		errorHandler: errorHandler,
		done:         make(chan struct{}),
	}

	hs := valuerpc.NewHandshakeRequest(clientId, resumeToken)
	if credential != nil {
		hs = hs.Put(valuerpc.DefaultDialect.AuthField, credential)
	}

	go t.requestLoop()
	t.SendRequest(hs)

	// Read until the handshake outcome, synchronously, so the connection is
	// "established" only once the handshake completes and the session is marked
	// established (which flips the resumption chain from anchor to pre-images).
	// This avoids two hazards: a fast reconnect that fires before the session is
	// established (it would resend the anchor instead of advancing the chain),
	// and a rejected handshake surfacing as a BadConnection (which would trigger
	// a reconnect storm) rather than a clean connect error. Bound by the dial ctx.
	//
	// On a session resume the server's sender may emit frames for the previous
	// connection's requests before its handshake reply reaches the wire, so
	// non-handshake messages here are dispatched normally and the read continues.
	// An ErrorResponse on the handshake request id is the server refusing the
	// handshake (bad credential, rejected resume, version mismatch): surface its
	// coded error from Connect instead of an opaque EOF from the closed conn.
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(dl)
	}
	for {
		resp, err := conn.ReadMessage()
		if err != nil {
			t.Close()
			return nil, xerrors.Errorf("handshake: %w", err)
		}
		mt, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.MessageTypeField)
		if ok && valuerpc.MessageType(mt.Long()) == valuerpc.ErrorResponse {
			if id, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.RequestIdField); ok &&
				id.Long() == valuerpc.DefaultDialect.HandshakeRequestId {
				t.Close()
				code := valuerpc.CodeUnknown
				if c, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.CodeField); ok {
					code = valuerpc.Code(c.Long())
				}
				msg := resp.GetString(valuerpc.DefaultDialect.ErrorField).String()
				return nil, xerrors.Errorf("handshake: %w", &valuerpc.Error{Code: code, Message: msg})
			}
		}
		t.respHandler(resp) // HandshakeResponse marks the session established
		if ok && valuerpc.MessageType(mt.Long()) == valuerpc.HandshakeResponse {
			break
		}
	}
	_ = conn.SetReadDeadline(time.Time{}) // clear; the response loop reads without a deadline

	go t.responseLoop()

	return t, nil
}

func (t *rpcConn) Close() error {
	// BUG-3 fix: signal shutdown via done instead of close(reqCh); SendRequest
	// is called from many goroutines and closing reqCh would panic them.
	t.closeOnce.Do(func() {
		close(t.done)
	})
	return t.conn.Close()
}

func (t *rpcConn) Stats() (int, int) {
	return len(t.reqCh), cap(t.reqCh)
}

func (t *rpcConn) requestLoop() {
	var batch []value.Map
	for {
		select {
		case <-t.done:
			return
		case req := <-t.reqCh:
			// Coalesce whatever is already queued — never waiting for more — so
			// a burst (e.g. stream values) costs one syscall, not one each.
			batch = append(batch[:0], req)
			for drain := true; drain && len(batch) < valuerpc.MaxCoalesce; {
				select {
				case more := <-t.reqCh:
					batch = append(batch, more)
				default:
					drain = false
				}
			}
			if err := valuerpc.WriteBatch(t.conn, batch); err != nil {
				if t.closedIntentionally() {
					return
				}
				t.errorHandler.BadConnection(err)
			}
		}
	}
}

func (t *rpcConn) responseLoop() error {

	for {

		resp, err := t.conn.ReadMessage()
		if err != nil {
			// A read error after this connection was intentionally closed (Close /
			// reset, e.g. during a reconnect) is expected — do not report it as a
			// bad connection, which would trigger a second, racing reconnect.
			if t.closedIntentionally() {
				return nil
			}
			t.errorHandler.BadConnection(err)
			return err
		}

		t.respHandler(resp)

	}

}

// closedIntentionally reports whether this connection was torn down on purpose
// (its done channel is closed) rather than failing on its own.
func (t *rpcConn) closedIntentionally() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func (t *rpcConn) SendRequest(req value.Map) {
	select {
	case t.reqCh <- req:
	case <-t.done:
	}
}
