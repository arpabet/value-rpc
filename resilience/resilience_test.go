/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package resilience_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/resilience"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

var bg = context.Background()

// seqInvoker returns the i-th programmed (value, error) on each call, counting
// invocations; the last entry repeats.
func seqInvoker(calls *int32, outcomes ...struct {
	v   value.Value
	err error
}) vrpc.Invoker {
	return func(context.Context, string, value.Value) (value.Value, error) {
		n := int(atomic.AddInt32(calls, 1)) - 1
		if n >= len(outcomes) {
			n = len(outcomes) - 1
		}
		return outcomes[n].v, outcomes[n].err
	}
}

type out = struct {
	v   value.Value
	err error
}

func ok(s string) out          { return out{value.Utf8(s), nil} }
func failCode(c vrpc.Code) out { return out{nil, vrpc.NewError(c, "x")} }

func TestRetrySucceedsAfterTransient(t *testing.T) {
	var calls int32
	inv := seqInvoker(&calls, failCode(vrpc.CodeUnavailable), failCode(vrpc.CodeUnavailable), ok("done"))
	ic := resilience.Retry(resilience.WithMaxAttempts(3), resilience.WithBackoff(time.Millisecond, 2*time.Millisecond))

	res, err := ic(bg, "m", nil, inv)
	if err != nil || res.(value.String).String() != "done" {
		t.Fatalf("res=%v err=%v", res, err)
	}
	if calls != 3 {
		t.Fatalf("calls=%d, want 3", calls)
	}
}

func TestRetryGivesUpAndStopsOnNonRetryable(t *testing.T) {
	// Non-retryable code: one attempt only.
	var calls int32
	ic := resilience.Retry(resilience.WithMaxAttempts(5), resilience.WithBackoff(time.Millisecond, time.Millisecond))
	_, err := ic(bg, "m", nil, seqInvoker(&calls, failCode(vrpc.CodeInvalidArgument)))
	if vrpc.CodeOf(err) != vrpc.CodeInvalidArgument || calls != 1 {
		t.Fatalf("non-retryable: code=%v calls=%d, want InvalidArgument/1", vrpc.CodeOf(err), calls)
	}

	// Retryable but always failing: exactly maxAttempts.
	calls = 0
	_, err = ic(bg, "m", nil, seqInvoker(&calls, failCode(vrpc.CodeUnavailable)))
	if vrpc.CodeOf(err) != vrpc.CodeUnavailable || calls != 5 {
		t.Fatalf("exhausted: code=%v calls=%d, want Unavailable/5", vrpc.CodeOf(err), calls)
	}
}

func TestRetryHonoursContextCancel(t *testing.T) {
	var calls int32
	ic := resilience.Retry(resilience.WithMaxAttempts(10), resilience.WithBackoff(time.Second, time.Second), resilience.WithJitter(false))
	ctx, cancel := context.WithCancel(bg)
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := ic(ctx, "m", nil, seqInvoker(&calls, failCode(vrpc.CodeUnavailable)))
	if vrpc.CodeOf(err) != vrpc.CodeUnavailable {
		t.Fatalf("code=%v, want Unavailable", vrpc.CodeOf(err))
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("retry did not abort on cancel: took %v", d)
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want 1 (cancelled during first backoff)", calls)
	}
}

func TestCircuitBreakerTripsAndRecovers(t *testing.T) {
	var calls int32
	failing := func(context.Context, string, value.Value) (value.Value, error) {
		atomic.AddInt32(&calls, 1)
		return nil, vrpc.NewError(vrpc.CodeUnavailable, "down")
	}
	ic := resilience.CircuitBreaker(
		resilience.WithFailureThreshold(2),
		resilience.WithCooldown(30*time.Millisecond),
	)

	// Two failures trip the breaker.
	ic(bg, "m", nil, failing)
	ic(bg, "m", nil, failing)
	if calls != 2 {
		t.Fatalf("calls=%d, want 2", calls)
	}
	// Now open: the next call fast-fails without invoking next.
	if _, err := ic(bg, "m", nil, failing); vrpc.CodeOf(err) != vrpc.CodeUnavailable {
		t.Fatalf("open: code=%v, want Unavailable", vrpc.CodeOf(err))
	}
	if calls != 2 {
		t.Fatalf("breaker did not fast-fail: calls=%d, want still 2", calls)
	}
	// After cooldown: half-open admits a probe; success closes it.
	time.Sleep(40 * time.Millisecond)
	if _, err := ic(bg, "m", nil, func(context.Context, string, value.Value) (value.Value, error) {
		atomic.AddInt32(&calls, 1)
		return value.Utf8("ok"), nil
	}); err != nil {
		t.Fatalf("half-open probe: %v", err)
	}
	if calls != 3 {
		t.Fatalf("probe not admitted: calls=%d, want 3", calls)
	}
}

func TestCircuitBreakerIgnoresClientErrors(t *testing.T) {
	var calls int32
	ic := resilience.CircuitBreaker(resilience.WithFailureThreshold(2))
	bad := seqInvoker(&calls, failCode(vrpc.CodeInvalidArgument))
	for i := 0; i < 5; i++ {
		ic(bg, "m", nil, bad)
	}
	// Caller faults must not trip the breaker — every call reaches next.
	if calls != 5 {
		t.Fatalf("calls=%d, want 5 (client errors should not trip)", calls)
	}
}

func TestTimeoutSetsDeadline(t *testing.T) {
	ic := resilience.Timeout(50 * time.Millisecond)
	_, _ = ic(bg, "m", nil, func(ctx context.Context, _ string, _ value.Value) (value.Value, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("expected a deadline on the call context")
		}
		return value.Null, nil
	})
}

func TestFallback(t *testing.T) {
	ic := resilience.Fallback(func(_ context.Context, _ string, _ value.Value, err error) (value.Value, error) {
		return value.Utf8("fallback"), nil
	})
	res, err := ic(bg, "m", nil, func(context.Context, string, value.Value) (value.Value, error) {
		return nil, vrpc.NewError(vrpc.CodeInternal, "boom")
	})
	if err != nil || res.(value.String).String() != "fallback" {
		t.Fatalf("res=%v err=%v, want fallback/nil", res, err)
	}
}

func TestBulkheadRejectsWhenFull(t *testing.T) {
	ic := resilience.Bulkhead(1)
	entered := make(chan struct{})
	release := make(chan struct{})
	go ic(bg, "m", nil, func(context.Context, string, value.Value) (value.Value, error) {
		close(entered)
		<-release
		return value.Utf8("ok"), nil
	})
	<-entered // the one slot is now held

	ctx, cancel := context.WithCancel(bg)
	cancel() // a waiter with a done context fails fast
	_, err := ic(ctx, "m", nil, func(context.Context, string, value.Value) (value.Value, error) {
		t.Error("next must not run while the bulkhead is full")
		return nil, nil
	})
	if vrpc.CodeOf(err) != vrpc.CodeResourceExhausted {
		t.Fatalf("code=%v, want ResourceExhausted", vrpc.CodeOf(err))
	}
	close(release)
}

func TestRateLimitShedsOverBurst(t *testing.T) {
	ic := resilience.RateLimit(1, 1) // 1/s, burst 1
	next := func(context.Context, string, value.Value) (value.Value, error) { return value.Utf8("ok"), nil }
	if _, err := ic(bg, "m", nil, next); err != nil {
		t.Fatalf("first call should pass: %v", err)
	}
	if _, err := ic(bg, "m", nil, next); vrpc.CodeOf(err) != vrpc.CodeResourceExhausted {
		t.Fatalf("second call code=%v, want ResourceExhausted", vrpc.CodeOf(err))
	}
}
