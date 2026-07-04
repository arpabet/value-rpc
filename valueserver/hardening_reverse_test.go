/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestReverseUnaryCancelPropagates verifies R5: when the server-side context of
// a reverse call is cancelled, the client's reverse handler observes the
// cancellation (a CancelRequest is sent), rather than running to completion with
// its reply discarded.
func TestReverseUnaryCancelPropagates(t *testing.T) {
	handlerCanceled := make(chan struct{}, 1)
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("triggerReverse", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				// Cancel the reverse call shortly after issuing it.
				cctx, cancel := context.WithCancel(ctx)
				go func() { time.Sleep(50 * time.Millisecond); cancel() }()
				_, err := peer.CallFunction(cctx, "blockUntilCanceled", nil)
				return value.Utf8("done"), err
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	cli.AddFunction("blockUntilCanceled", valuerpc.Any, valuerpc.Any,
		func(ctx context.Context, _ value.Value) (value.Value, error) {
			select {
			case <-ctx.Done():
				handlerCanceled <- struct{}{}
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return value.Utf8("should-not-happen"), nil
			}
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	_, _ = cli.CallFunction(context.Background(), "triggerReverse", nil)
	select {
	case <-handlerCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("reverse handler context was not cancelled (R5 cancel did not propagate)")
	}
}

// TestClientCloseUnblocksInFlight verifies R7: closing the client fails an
// in-flight unary call and closes an in-flight stream's channel, rather than
// leaving callers hung.
func TestClientCloseUnblocksInFlight(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("hang", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
		s.AddOutgoingStream("slowfeed", valuerpc.Any,
			func(ctx context.Context, _ value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					<-ctx.Done() // never sends; stays open until torn down
				}()
				return out, nil
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	cli.SetTimeout(10000)

	// In-flight unary.
	unaryErr := make(chan error, 1)
	go func() {
		_, err := cli.CallFunction(context.Background(), "hang", nil)
		unaryErr <- err
	}()

	// In-flight stream: consume the channel.
	readC, _, err := cli.GetStream(context.Background(), "slowfeed", nil, 4)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	streamClosed := make(chan struct{})
	go func() {
		for range readC {
		}
		close(streamClosed)
	}()

	time.Sleep(50 * time.Millisecond) // let both reach their blocked state
	cli.Close()

	select {
	case err := <-unaryErr:
		if valuerpc.CodeOf(err) != valuerpc.CodeUnavailable {
			t.Fatalf("in-flight unary err = %v (code %v), want CodeUnavailable", err, valuerpc.CodeOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight unary did not unblock on Close (R7)")
	}
	select {
	case <-streamClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight stream channel did not close on Close (R7)")
	}
}

// TestReverseCallConcurrencyBound verifies R6: the client rejects reverse calls
// over its concurrency limit with CodeResourceExhausted rather than spawning
// unbounded goroutines.
func TestReverseCallConcurrencyBound(t *testing.T) {
	var rejected atomic.Int64
	release := make(chan struct{})
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("fanout", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				// Fire several concurrent reverse calls; some must be rejected.
				results := make(chan error, 4)
				for i := 0; i < 4; i++ {
					go func() {
						_, err := peer.CallFunction(ctx, "slowClient", nil)
						results <- err
					}()
				}
				for i := 0; i < 4; i++ {
					if err := <-results; valuerpc.CodeOf(err) == valuerpc.CodeResourceExhausted {
						rejected.Add(1)
					}
				}
				return value.Utf8("ok"), nil
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "", valueclient.WithMaxConcurrentInbound(2))
	cli.AddFunction("slowClient", valuerpc.Any, valuerpc.Any,
		func(ctx context.Context, _ value.Value) (value.Value, error) {
			<-release // hold the slot so concurrency is real
			return value.Utf8("v"), nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	done := make(chan struct{})
	go func() {
		_, _ = cli.CallFunction(context.Background(), "fanout", nil)
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	close(release) // let the admitted handlers finish
	<-done

	if rejected.Load() < 1 {
		t.Fatalf("expected at least one reverse call rejected over the limit, got %d", rejected.Load())
	}
}
