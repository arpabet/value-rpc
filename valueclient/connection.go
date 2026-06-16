/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"sync"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
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

func newConn(dialer valuerpc.Dialer, clientId int64, sessionToken string, sendingCap int64, respHandler responseHandler, errorHandler ErrorHandler) (*rpcConn, error) {

	conn, err := dialer.Dial()
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

	go t.requestLoop()
	t.SendRequest(valuerpc.NewHandshakeRequest(clientId, sessionToken))
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
	for {
		select {
		case <-t.done:
			return
		case req := <-t.reqCh:
			if err := t.conn.WriteMessage(req); err != nil {
				t.errorHandler.BadConnection(err)
			}
		}
	}
}

func (t *rpcConn) responseLoop() error {

	for {

		resp, err := t.conn.ReadMessage()
		if err != nil {
			t.errorHandler.BadConnection(err)
			return err
		}

		t.respHandler(resp)

	}

}

func (t *rpcConn) SendRequest(req value.Map) {
	select {
	case t.reqCh <- req:
	case <-t.done:
	}
}
