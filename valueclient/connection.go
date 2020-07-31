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

package valueclient

import (
	"github.com/consensusdb/value"
	"github.com/consensusdb/value-rpc/valuerpc"
	"golang.org/x/net/proxy"
	"net"
)

/**
@author Alex Shvid
*/

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
