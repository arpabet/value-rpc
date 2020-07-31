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
	"sync"
	"sync/atomic"
)

/**
@author Alex Shvid
*/

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
