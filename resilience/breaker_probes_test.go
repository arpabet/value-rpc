/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package resilience_test

import (
	"context"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/resilience"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

func okFn(context.Context, string, value.Value) (value.Value, error) {
	return value.Utf8("ok"), nil
}
func downFn(context.Context, string, value.Value) (value.Value, error) {
	return nil, vrpc.NewError(vrpc.CodeUnavailable, "down")
}

// tripBreaker drives threshold consecutive failures to open the breaker.
func tripBreaker(t *testing.T, ic vrpc.ClientInterceptor, threshold int) {
	t.Helper()
	for i := 0; i < threshold; i++ {
		ic(bg, "m", nil, downFn)
	}
	// Confirm it is open (fast-fails without invoking next).
	invoked := false
	ic(bg, "m", nil, func(context.Context, string, value.Value) (value.Value, error) {
		invoked = true
		return value.Null, nil
	})
	if invoked {
		t.Fatal("breaker did not open after threshold failures")
	}
}

// TestCircuitBreakerRequiresAllProbes verifies WithHalfOpenProbes(n) keeps the
// breaker HALF-OPEN until n probes succeed (documented semantics): a probe
// FAILURE after fewer than n successes re-opens the breaker. With a high failure
// threshold this distinguishes "still half-open" (a single failure re-opens)
// from "already closed after one success" (a single failure only increments the
// closed-state counter and is not enough to re-trip).
func TestCircuitBreakerRequiresAllProbes(t *testing.T) {
	const threshold = 5
	ic := resilience.CircuitBreaker(
		resilience.WithFailureThreshold(threshold),
		resilience.WithCooldown(20*time.Millisecond),
		resilience.WithHalfOpenProbes(3),
	)
	tripBreaker(t, ic, threshold)
	time.Sleep(30 * time.Millisecond) // cooldown elapses -> half-open

	// One probe succeeds. With maxProbes=3 the breaker must still be HALF-OPEN.
	if _, err := ic(bg, "m", nil, okFn); err != nil {
		t.Fatalf("first probe should be admitted: %v", err)
	}
	// A single failing probe now must RE-OPEN the breaker (half-open failure),
	// not merely bump a closed-state counter.
	if _, err := ic(bg, "m", nil, downFn); vrpc.CodeOf(err) != vrpc.CodeUnavailable {
		t.Fatalf("failing probe should pass through, code=%v", vrpc.CodeOf(err))
	}
	// If half-open semantics hold, the breaker is now OPEN and fast-fails.
	invoked := false
	if _, err := ic(bg, "m", nil, func(context.Context, string, value.Value) (value.Value, error) {
		invoked = true
		return value.Null, nil
	}); vrpc.CodeOf(err) != vrpc.CodeUnavailable {
		t.Fatalf("after a half-open probe failure the breaker must be open, code=%v", vrpc.CodeOf(err))
	}
	if invoked {
		t.Fatal("breaker closed after a single probe success (WithHalfOpenProbes(3) not honoured)")
	}
}

// TestCircuitBreakerClosesAfterAllProbes verifies the breaker DOES close once n
// probes all succeed, and then admits traffic freely.
func TestCircuitBreakerClosesAfterAllProbes(t *testing.T) {
	const threshold = 5
	ic := resilience.CircuitBreaker(
		resilience.WithFailureThreshold(threshold),
		resilience.WithCooldown(20*time.Millisecond),
		resilience.WithHalfOpenProbes(3),
	)
	tripBreaker(t, ic, threshold)
	time.Sleep(30 * time.Millisecond)

	// Three successful probes close the breaker.
	for i := 0; i < 3; i++ {
		if _, err := ic(bg, "m", nil, okFn); err != nil {
			t.Fatalf("probe %d should be admitted: %v", i+1, err)
		}
	}
	// Closed: a single failure does not fast-fail (needs `threshold` in a row),
	// so the next call reaches next().
	invoked := false
	ic(bg, "m", nil, func(context.Context, string, value.Value) (value.Value, error) {
		invoked = true
		return nil, vrpc.NewError(vrpc.CodeUnavailable, "x")
	})
	if !invoked {
		t.Fatal("breaker should be closed after all probes succeeded, but fast-failed")
	}
}
