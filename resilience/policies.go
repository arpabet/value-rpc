/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package resilience

import (
	"context"
	"sync"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// Timeout returns an interceptor that bounds each call (or each attempt, when
// placed inside Retry) with a context deadline. The client sends the deadline to
// the server as the request SLA, so a cooperating handler observes it too.
func Timeout(d time.Duration) valuerpc.ClientInterceptor {
	return func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return next(ctx, method, req)
	}
}

// Fallback returns an interceptor that, when the call fails, returns the result
// of fn instead — e.g. a cached or default value, or a transformed error.
func Fallback(fn func(ctx context.Context, method string, req value.Value, err error) (value.Value, error)) valuerpc.ClientInterceptor {
	return func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
		res, err := next(ctx, method, req)
		if err != nil {
			return fn(ctx, method, req, err)
		}
		return res, nil
	}
}

// Bulkhead returns an interceptor that caps the number of concurrent in-flight
// calls through it at maxConcurrent, isolating a slow dependency from exhausting
// resources. A call waits for a slot, honouring the context deadline/cancellation
// (a bounded context turns this into fail-fast). maxConcurrent < 1 is treated as 1.
func Bulkhead(maxConcurrent int) valuerpc.ClientInterceptor {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	sem := make(chan struct{}, maxConcurrent)
	return func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			return nil, valuerpc.NewError(valuerpc.CodeResourceExhausted, "bulkhead full for %q: %v", method, ctx.Err())
		}
		return next(ctx, method, req)
	}
}

// tokenBucket is a lazily-refilled token bucket; see RateLimit.
type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	max    float64
	perSec float64
	last   time.Time
}

func (t *tokenBucket) allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.tokens += now.Sub(t.last).Seconds() * t.perSec
	if t.tokens > t.max {
		t.tokens = t.max
	}
	t.last = now
	if t.tokens >= 1 {
		t.tokens--
		return true
	}
	return false
}

// RateLimit returns an interceptor that caps the outbound call rate at
// perSecond, allowing short bursts up to burst. Calls over the limit fail fast
// with CodeResourceExhausted (load shedding) rather than queueing.
func RateLimit(perSecond float64, burst int) valuerpc.ClientInterceptor {
	if burst < 1 {
		burst = 1
	}
	tb := &tokenBucket{tokens: float64(burst), max: float64(burst), perSec: perSecond, last: time.Now()}
	return func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
		if !tb.allow() {
			return nil, valuerpc.NewError(valuerpc.CodeResourceExhausted, "rate limit exceeded for %q", method)
		}
		return next(ctx, method, req)
	}
}
