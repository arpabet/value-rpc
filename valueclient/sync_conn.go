/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
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
