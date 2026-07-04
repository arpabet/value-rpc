/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"sync"
	"sync/atomic"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

type syncConn struct {
	connecting sync.Mutex
	active     *sync.Cond
	conn       atomic.Value
	closed     atomic.Bool // set by close(); makes getConn return nil instead of blocking forever
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

func (t *syncConn) connect(ctx context.Context, dialer valuerpc.Dialer, clientId int64, resumeToken string, credential value.Value, sendingCap int64, respHandler responseHandler, errorHandler ErrorHandler) error {

	t.connecting.Lock()
	defer t.connecting.Unlock()

	if t.hasConn() {
		return nil
	}

	conn, err := newConn(ctx, dialer, clientId, resumeToken, credential, sendingCap, respHandler, errorHandler)
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
		// Once the client is closed there will never be a connection again;
		// return nil so a blocked sender (e.g. streamOut) unblocks instead of
		// waiting on the cond forever (R7). Callers must nil-check.
		if t.closed.Load() {
			return nil
		}
		t.active.Wait()
	}
}

// close permanently marks the connection provider shut down and wakes any
// getConn waiter, which then returns nil. Idempotent.
func (t *syncConn) close() {
	t.connecting.Lock()
	t.closed.Store(true)
	t.active.Broadcast()
	t.connecting.Unlock()
}

func (t *syncConn) reset() {
	conn := t.conn.Load().(connHolder)
	t.conn.Store(connHolder{nil})
	if conn.value != nil {
		conn.value.Close()
	}
}
