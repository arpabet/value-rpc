/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"sync"

	"go.arpabet.com/value"
)

// DefaultMaxPending bounds a single request's internal pending queue inside a
// StreamPump. A consumer that falls more than this many values behind is
// treated as failed (the stream is closed) rather than letting a peer pin
// unbounded memory. Cooperative throttling (ThrottleIncrease) normally keeps
// well-behaved peers from ever reaching the bound.
var DefaultMaxPending = 4096

// StreamPump decouples a non-blocking producer (the shared connection
// read/response loop, which multiplexes every request on one socket) from a
// possibly-slow consumer (a single application stream channel).
//
// This is the fix for the head-of-line blocking defect (BUG-6): the loop used
// to deliver each value with a blocking channel send, so one slow consumer
// froze every other multiplexed request — and every control message (cancel,
// throttle) — on the connection. With a pump, Push never blocks the caller; a
// dedicated goroutine drains the pending queue into the out channel at the
// consumer's pace, so backpressure is isolated to the one slow request.
//
// The out channel is created and closed by the owner-side code, but only the
// pump goroutine ever closes it (exactly once, when it finishes), so there is
// no send-on-closed or double-close. The pending queue is bounded by
// maxPending: exceeding it fails the stream instead of growing without bound.
type StreamPump struct {
	out chan value.Value

	mu         sync.Mutex
	cond       *sync.Cond
	queue      []value.Value
	head       int
	inputEnd   bool // Close(): no more Push; drain the queue then finish
	overflow   bool // exceeded maxPending: the consumer was too slow
	maxPending int

	onDeliver func() // called once per value delivered to out (credit replenishment)

	stopCh   chan struct{} // Stop(): abandon and finish immediately
	stopOnce sync.Once
}

// NewStreamPump starts a pump that delivers pushed values into out, in order,
// at the consumer's pace. out is closed by the pump when it finishes. A
// maxPending <= 0 uses DefaultMaxPending. onDeliver (may be nil) is invoked once
// per value handed to out, so a receiver can replenish the sender's flow-control
// credit as its buffer drains.
func NewStreamPump(out chan value.Value, maxPending int, onDeliver func()) *StreamPump {
	if maxPending <= 0 {
		maxPending = DefaultMaxPending
	}
	p := &StreamPump{
		out:        out,
		maxPending: maxPending,
		onDeliver:  onDeliver,
		stopCh:     make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	go p.run()
	return p
}

// Push enqueues v for delivery and never blocks the caller. It returns false if
// the pump has finished (Close/Stop) or the pending bound was exceeded — in
// which case the caller should stop producing for this request.
func (p *StreamPump) Push(v value.Value) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inputEnd || p.isStopped() {
		return false
	}
	if len(p.queue)-p.head >= p.maxPending {
		p.overflow = true
		p.inputEnd = true // drain what we have, then close
		p.cond.Signal()
		return false
	}
	if p.head > 0 && len(p.queue) == cap(p.queue) {
		// Compact in place so the backing array is not grown forever under
		// sustained backpressure; the live region is queue[head:].
		n := copy(p.queue, p.queue[p.head:])
		for i := n; i < len(p.queue); i++ {
			p.queue[i] = nil
		}
		p.queue = p.queue[:n]
		p.head = 0
	}
	p.queue = append(p.queue, v)
	p.cond.Signal()
	return true
}

// Close signals end-of-input: the pump drains whatever is queued into out, then
// closes out and exits. Use it for a normal end of stream. Idempotent.
func (p *StreamPump) Close() {
	p.mu.Lock()
	p.inputEnd = true
	p.cond.Signal()
	p.mu.Unlock()
}

// Stop abandons any queued values, closes out, and exits immediately. Use it on
// hard teardown (cancel, connection drop, shutdown), where the consumer may
// have stopped reading and a graceful drain would block forever. Idempotent.
func (p *StreamPump) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.mu.Lock()
	p.cond.Signal()
	p.mu.Unlock()
}

// Overflowed reports whether the pending bound was exceeded (the consumer fell
// too far behind). Meaningful once the pump has finished.
func (p *StreamPump) Overflowed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.overflow
}

func (p *StreamPump) isStopped() bool {
	select {
	case <-p.stopCh:
		return true
	default:
		return false
	}
}

func (p *StreamPump) run() {
	defer close(p.out)
	for {
		p.mu.Lock()
		for len(p.queue)-p.head == 0 && !p.inputEnd && !p.isStopped() {
			p.cond.Wait()
		}
		if p.isStopped() {
			p.mu.Unlock()
			return
		}
		if len(p.queue)-p.head == 0 {
			p.mu.Unlock()
			return // drained and input ended
		}
		v := p.queue[p.head]
		p.queue[p.head] = nil
		p.head++
		if p.head == len(p.queue) {
			p.queue = p.queue[:0]
			p.head = 0
		}
		p.mu.Unlock()

		// Only this pump goroutine blocks while the consumer is slow — never the
		// shared connection loop. Stop() interrupts a stuck send.
		select {
		case p.out <- v:
			if p.onDeliver != nil {
				p.onDeliver()
			}
		case <-p.stopCh:
			return
		}
	}
}
