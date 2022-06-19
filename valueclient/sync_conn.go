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
	"sync"
	"sync/atomic"
)


type syncConn struct {
	connecting sync.Mutex
	active     *sync.Cond
	conn       atomic.Value
}

type connHolder struct {
	value *rpcConn
}

func NewSyncConn() *syncConn {

	t := &syncConn{}
	t.active = sync.NewCond(&t.connecting)
	t.conn.Store(connHolder{nil})
	return t
}

func (t *syncConn) connect(address, socks5 string, clientId, sendingCap int64, respHandler responseHandler, errorHandler ErrorHandler) error {

	t.connecting.Lock()
	defer t.connecting.Unlock()

	if t.hasConn() {
		return nil
	}

	conn, err := newConn(address, socks5, clientId, sendingCap, respHandler, errorHandler)
	if err != nil {
		return err
	}

	t.conn.Store(connHolder{conn})
	t.active.Broadcast()

	return nil
}

func (t *syncConn) hasConn() bool {
	return t.conn.Load().(connHolder).value != nil
}

func (t *syncConn) getConn() *rpcConn {
	conn := t.conn.Load().(connHolder)
	if conn.value == nil {
		t.active.Wait()
		return t.getConn()
	}
	return conn.value
}

func (t *syncConn) reset() {
	conn := t.conn.Load().(connHolder)
	t.conn.Store(connHolder{nil})
	if conn.value != nil {
		conn.value.Close()
	}
}
