/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"sync"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// TestStreamPump_DeliversInOrderAndCloses checks the happy path: every pushed
// value reaches the consumer in order, and Close() closes the out channel once
// the queue is drained.
func TestStreamPump_DeliversInOrderAndCloses(t *testing.T) {
	out := make(chan value.Value, 4)
	p := valuerpc.NewStreamPump(out, 0)

	const n = 100
	for i := 0; i < n; i++ {
		if !p.Push(value.Long(int64(i))) {
			t.Fatalf("push %d returned false", i)
		}
	}
	p.Close()

	got := 0
	for v := range out {
		if v.(value.Number).Long() != int64(got) {
			t.Fatalf("out of order at %d: got %d", got, v.(value.Number).Long())
		}
		got++
	}
	if got != n {
		t.Fatalf("delivered %d values, want %d", got, n)
	}
	if p.Overflowed() {
		t.Fatal("pump reported overflow on the happy path")
	}
}

// TestStreamPump_PushNeverBlocks is the core BUG-6 property: Push must return
// promptly even when the consumer has stopped reading and the out buffer is
// full. (A blocking enqueue here is exactly what froze the shared loop.)
func TestStreamPump_PushNeverBlocks(t *testing.T) {
	out := make(chan value.Value) // unbuffered; nobody reads it
	p := valuerpc.NewStreamPump(out, 8)

	done := make(chan struct{})
	go func() {
		// Far more than maxPending; must not block even though out is stuck.
		for i := 0; i < 1000; i++ {
			p.Push(value.Long(int64(i)))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Push blocked the producer: head-of-line blocking regression")
	}

	if !p.Overflowed() {
		t.Fatal("expected overflow when the consumer never reads")
	}
	p.Stop() // unblock and reap the pump goroutine
}

// TestStreamPump_StopUnblocksStuckDelivery verifies Stop() reaps a pump goroutine
// blocked delivering to a consumer that walked away, closing out.
func TestStreamPump_StopUnblocksStuckDelivery(t *testing.T) {
	out := make(chan value.Value) // unbuffered, never read
	p := valuerpc.NewStreamPump(out, 16)

	p.Push(value.Long(1)) // pump goroutine will block on out <- v

	p.Stop()

	select {
	case _, ok := <-out:
		if ok {
			// a value may race through before stop; the channel must still close
			if _, ok2 := <-out; ok2 {
				t.Fatal("out not closed after Stop")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not close out / reap the pump")
	}
}

// TestStreamPump_ConcurrentPushClose runs the race detector against concurrent
// producers and a Close.
func TestStreamPump_ConcurrentPushClose(t *testing.T) {
	out := make(chan value.Value, 16)
	p := valuerpc.NewStreamPump(out, 0)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				p.Push(value.Long(int64(i)))
			}
		}()
	}
	// Drain concurrently so producers are not permanently blocked by overflow.
	drained := make(chan struct{})
	go func() {
		for range out {
		}
		close(drained)
	}()
	wg.Wait()
	p.Close()
	<-drained
}
