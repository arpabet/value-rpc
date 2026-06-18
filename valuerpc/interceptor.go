/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"context"

	"go.arpabet.com/value"
)

// Invoker performs a unary call: it sends req to the named function and returns
// the result. It is the "next" step a ClientInterceptor wraps; the innermost
// Invoker in a chain is the client's actual network call.
type Invoker func(ctx context.Context, method string, req value.Value) (value.Value, error)

// ClientInterceptor wraps a unary client call. It can inspect or modify the
// context and request, short-circuit without calling next (e.g. a tripped circuit
// breaker or a rate limiter), invoke next zero or more times (e.g. retry or
// hedging), and inspect or transform the result and error (e.g. a fallback).
//
// Interceptors compose into a chain via valueclient.WithInterceptors; the first
// interceptor is outermost (it runs first and wraps the rest). This is the seam
// service-governance policies plug into — retries, circuit breaking, deadline
// budgets, fallback, client-side rate limiting. Core ships only the seam: the
// policies themselves live outside core (e.g. go.arpabet.com/value-rpc/resilience)
// so they impose no dependencies on plain clients. Interceptors classify outcomes
// with the wire-agnostic Code model (CodeOf): Unavailable/DeadlineExceeded/
// ResourceExhausted are typically retryable; InvalidArgument/NotFound/
// Unauthenticated are not.
type ClientInterceptor func(ctx context.Context, method string, req value.Value, next Invoker) (value.Value, error)

// ChainClientInterceptors composes interceptors around final into a single
// Invoker. interceptors[0] is outermost; an empty list returns final unchanged.
func ChainClientInterceptors(final Invoker, interceptors ...ClientInterceptor) Invoker {
	invoker := final
	for i := len(interceptors) - 1; i >= 0; i-- {
		ic, next := interceptors[i], invoker
		invoker = func(ctx context.Context, method string, req value.Value) (value.Value, error) {
			return ic(ctx, method, req, next)
		}
	}
	return invoker
}
