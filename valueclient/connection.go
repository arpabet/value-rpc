/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
*/

package valueclient

import (
	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"golang.org/x/net/proxy"
	"net"
)


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
		conn:         valuerpc.NewMsgConn(conn),
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
