/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package resilience

import (
	"context"
	"math/rand/v2"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

type retryConfig struct {
	maxAttempts int
	base        time.Duration
	max         time.Duration
	jitter      bool
	retryable   func(method string, err error) bool
}

// RetryOption configures Retry.
type RetryOption func(*retryConfig)

// WithMaxAttempts sets the total number of attempts including the first (default
// 3). Values < 1 are treated as 1 (no retry).
func WithMaxAttempts(n int) RetryOption {
	return func(c *retryConfig) {
		if n < 1 {
			n = 1
		}
		c.maxAttempts = n
	}
}

// WithBackoff sets the base and maximum backoff between attempts (default
// 50ms..1s). The delay doubles each attempt, capped at max.
func WithBackoff(base, max time.Duration) RetryOption {
	return func(c *retryConfig) { c.base, c.max = base, max }
}

// WithJitter enables/disables equal jitter on the backoff (default on).
func WithJitter(on bool) RetryOption {
	return func(c *retryConfig) { c.jitter = on }
}

// WithRetryable replaces the predicate deciding whether a failed call to method
// should be retried. The default retries only CodeUnavailable and
// CodeResourceExhausted (the server did not process the request, so retrying is
// safe regardless of idempotency). Widening this to codes that may have executed
// makes retry at-least-once — restrict it to idempotent methods.
func WithRetryable(f func(method string, err error) bool) RetryOption {
	return func(c *retryConfig) { c.retryable = f }
}

func defaultRetryable(_ string, err error) bool {
	switch valuerpc.CodeOf(err) {
	case valuerpc.CodeUnavailable, valuerpc.CodeResourceExhausted:
		return true
	default:
		return false
	}
}

// Retry returns an interceptor that re-invokes a failed call with exponential
// backoff while the retryable predicate holds, up to maxAttempts. Backoff waits
// honour the context deadline/cancellation.
func Retry(opts ...RetryOption) valuerpc.ClientInterceptor {
	cfg := retryConfig{
		maxAttempts: 3,
		base:        50 * time.Millisecond,
		max:         time.Second,
		jitter:      true,
		retryable:   defaultRetryable,
	}
	for _, o := range opts {
		o(&cfg)
	}

	return func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
		delay := cfg.base
		var res value.Value
		var err error
		for attempt := 1; ; attempt++ {
			res, err = next(ctx, method, req)
			if err == nil || attempt >= cfg.maxAttempts || !cfg.retryable(method, err) {
				return res, err
			}
			d := delay
			if cfg.jitter && d > 0 {
				d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
			}
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return res, err // give up on cancellation; surface the last result
			}
			if delay *= 2; delay > cfg.max {
				delay = cfg.max
			}
		}
	}
}
