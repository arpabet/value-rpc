/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueclient

import (
	"net"
	"sync"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"golang.org/x/net/proxy"
)

var DefaultTimeout = 10 * time.Second

// KeepAlivePeriod enables TCP keepalive so dead peers are detected and their
// goroutines/fds reclaimed without killing idle-but-healthy streams (BUG-10).
var KeepAlivePeriod = 15 * time.Second

type rpcConn struct {
	conn         valuerpc.MsgConn
	reqCh        chan value.Map
	respHandler  responseHandler
	errorHandler ErrorHandler
	done         chan struct{}
	closeOnce    sync.Once
}

func dial(address, socks5 string) (net.Conn, error) {
	if socks5 != "" {
		d, err := proxy.SOCKS5("tcp", socks5, nil, proxy.Direct)
		if err != nil {
			return nil, err
		}
		return d.Dial("tcp", address)
	} else {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return nil, err
		}
		enableKeepAlive(conn)
		return conn, nil
	}
}

func enableKeepAlive(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok && KeepAlivePeriod > 0 {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(KeepAlivePeriod)
	}
}

func newConn(address, socks5 string, clientId int64, sendingCap int64, respHandler responseHandler, errorHandler ErrorHandler) (*rpcConn, error) {

	conn, err := dial(address, socks5)
	if err != nil {
		return nil, err
	}

	t := &rpcConn{
		conn:         valuerpc.NewMsgConn(conn, DefaultTimeout),
		reqCh:        make(chan value.Map, sendingCap),
		respHandler:  respHandler,
		errorHandler: errorHandler,
		done:         make(chan struct{}),
	}

	go t.requestLoop()
	t.SendRequest(valuerpc.NewHandshakeRequest(clientId))
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
