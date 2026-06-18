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

// breakerState is a circuit-breaker state.
type breakerState int

const (
	stateClosed   breakerState = iota // calls flow; failures are counted
	stateOpen                         // calls fail fast until the cooldown elapses
	stateHalfOpen                     // a few probe calls are allowed to test recovery
)

type breakerConfig struct {
	threshold int           // consecutive failures that trip the breaker
	cooldown  time.Duration // how long to stay open before probing
	maxProbes int           // concurrent probes allowed in half-open
	isFailure func(err error) bool
}

// BreakerOption configures CircuitBreaker.
type BreakerOption func(*breakerConfig)

// WithFailureThreshold sets the number of consecutive failures that trips the
// breaker (default 5).
func WithFailureThreshold(n int) BreakerOption {
	return func(c *breakerConfig) {
		if n < 1 {
			n = 1
		}
		c.threshold = n
	}
}

// WithCooldown sets how long the breaker stays open before allowing probes
// (default 10s).
func WithCooldown(d time.Duration) BreakerOption {
	return func(c *breakerConfig) { c.cooldown = d }
}

// WithHalfOpenProbes sets how many probe calls are allowed in the half-open state
// (default 1). The breaker closes only when all probes succeed.
func WithHalfOpenProbes(n int) BreakerOption {
	return func(c *breakerConfig) {
		if n < 1 {
			n = 1
		}
		c.maxProbes = n
	}
}

// WithIsFailure replaces the predicate deciding whether an outcome counts as a
// failure for the breaker. The default counts any error except caller faults
// (CodeInvalidArgument, CodeNotFound, CodeUnauthenticated, CodeCanceled), which
// indicate a bad request rather than an unhealthy peer.
func WithIsFailure(f func(err error) bool) BreakerOption {
	return func(c *breakerConfig) { c.isFailure = f }
}

func defaultIsFailure(err error) bool {
	if err == nil {
		return false
	}
	switch valuerpc.CodeOf(err) {
	case valuerpc.CodeInvalidArgument, valuerpc.CodeNotFound, valuerpc.CodeUnauthenticated, valuerpc.CodeCanceled:
		return false
	default:
		return true
	}
}

type breaker struct {
	cfg      *breakerConfig
	mu       sync.Mutex
	state    breakerState
	consec   int
	openedAt time.Time
	probes   int
}

// allow reports whether a call may proceed, advancing open→half-open after the
// cooldown and admitting up to maxProbes probes.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateOpen:
		if time.Since(b.openedAt) >= b.cfg.cooldown {
			b.state = stateHalfOpen
			b.probes = 1
			return true
		}
		return false
	case stateHalfOpen:
		if b.probes < b.cfg.maxProbes {
			b.probes++
			return true
		}
		return false
	default: // closed
		return true
	}
}

// record folds a call outcome into the breaker state.
func (b *breaker) record(failed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed:
		if failed {
			if b.consec++; b.consec >= b.cfg.threshold {
				b.trip()
			}
		} else {
			b.consec = 0
		}
	case stateHalfOpen:
		if failed {
			b.trip()
		} else if b.probes--; b.probes <= 0 {
			b.state = stateClosed
			b.consec = 0
		}
	}
}

func (b *breaker) trip() {
	b.state = stateOpen
	b.openedAt = time.Now()
	b.probes = 0
}

// CircuitBreaker returns an interceptor that trips a per-method breaker after
// consecutive failures and fast-fails further calls (CodeUnavailable) until a
// cooldown elapses, then probes for recovery.
func CircuitBreaker(opts ...BreakerOption) valuerpc.ClientInterceptor {
	cfg := &breakerConfig{
		threshold: 5,
		cooldown:  10 * time.Second,
		maxProbes: 1,
		isFailure: defaultIsFailure,
	}
	for _, o := range opts {
		o(cfg)
	}

	var breakers sync.Map // method -> *breaker

	return func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
		bi, _ := breakers.LoadOrStore(method, &breaker{cfg: cfg})
		b := bi.(*breaker)
		if !b.allow() {
			return nil, valuerpc.NewError(valuerpc.CodeUnavailable, "circuit breaker open for %q", method)
		}
		res, err := next(ctx, method, req)
		b.record(cfg.isFailure(err))
		return res, err
	}
}
