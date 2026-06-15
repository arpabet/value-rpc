/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueclient

import (
	"sync"
	"sync/atomic"

	"go.arpabet.com/value-rpc/valuerpc"
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

func (t *syncConn) connect(dialer valuerpc.Dialer, clientId, sendingCap int64, respHandler responseHandler, errorHandler ErrorHandler) error {

	t.connecting.Lock()
	defer t.connecting.Unlock()

	if t.hasConn() {
		return nil
	}

	conn, err := newConn(dialer, clientId, sendingCap, respHandler, errorHandler)
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
	// BUG-17 fix: sync.Cond.Wait requires its Locker to be held; the previous
	// code called Wait() with no lock, which panics ("unlock of unlocked
	// mutex"). Hold the lock and loop on the predicate.
	if conn := t.conn.Load().(connHolder); conn.value != nil {
		return conn.value
	}
	t.connecting.Lock()
	defer t.connecting.Unlock()
	for {
		conn := t.conn.Load().(connHolder)
		if conn.value != nil {
			return conn.value
		}
		t.active.Wait()
	}
}

func (t *syncConn) reset() {
	conn := t.conn.Load().(connHolder)
	t.conn.Store(connHolder{nil})
	if conn.value != nil {
		conn.value.Close()
	}
}
