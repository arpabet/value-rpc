/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"golang.org/x/net/proxy"
	"net"
	"time"
)

var DefaultTimeout  = 10 * time.Second

type rpcConn struct {
	conn         valuerpc.MsgConn
	reqCh        chan value.Map
	respHandler  responseHandler
	errorHandler ErrorHandler
}

func dial(address, socks5 string) (net.Conn, error) {
	if socks5 != "" {
		d, err := proxy.SOCKS5("tcp", socks5, nil, proxy.Direct)
		if err != nil {
			return nil, err
		}
		return d.Dial("tcp", address)
	} else {
		return net.Dial("tcp", address)
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
	}

	go t.requestLoop()
	t.SendRequest(valuerpc.NewHandshakeRequest(clientId))
	go t.responseLoop()

	return t, nil
}

func (t *rpcConn) Close() error {
	close(t.reqCh)
	return t.conn.Close()
}

func (t *rpcConn) Stats() (int, int) {
	return len(t.reqCh), cap(t.reqCh)
}

func (t *rpcConn) requestLoop() {

	for {
		req, ok := <-t.reqCh

		if !ok {
			break
		}

		err := t.conn.WriteMessage(req)
		if err != nil {
			t.errorHandler.BadConnection(err)
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
	t.reqCh <- req
}
