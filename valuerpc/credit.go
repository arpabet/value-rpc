/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import "sync"

// CreditGate is the sender side of credit-based stream flow control. A data
// sender calls Acquire before sending each stream value; it blocks (only the
// per-stream sender goroutine, never the shared connection loop) until the
// receiver has granted credit via Grant. Because the receiver only grants credit
// as it drains its buffer, the sender can never outrun the receiver — delivery is
// lossless and buffering stays bounded, with no head-of-line blocking. Close
// unblocks a waiting sender on teardown.
type CreditGate struct {
	mu     sync.Mutex
	cond   *sync.Cond
	credit int64
	closed bool
}

func NewCreditGate() *CreditGate {
	g := &CreditGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// Acquire waits for at least one credit, consumes it, and returns true. It
// returns false if the gate is closed (the request was torn down).
func (g *CreditGate) Acquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.credit <= 0 && !g.closed {
		g.cond.Wait()
	}
	if g.closed {
		return false
	}
	g.credit--
	return true
}

// Grant adds n credits and wakes any waiting sender.
func (g *CreditGate) Grant(n int64) {
	if n <= 0 {
		return
	}
	g.mu.Lock()
	g.credit += n
	g.cond.Broadcast()
	g.mu.Unlock()
}

// Close permanently unblocks Acquire (which returns false thereafter). Idempotent.
func (g *CreditGate) Close() {
	g.mu.Lock()
	g.closed = true
	g.cond.Broadcast()
	g.mu.Unlock()
}
