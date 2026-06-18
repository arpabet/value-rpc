/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Package resilience provides service-governance policies for vRPC clients as
// composable unary interceptors (valuerpc.ClientInterceptor): retry, circuit
// breaking, timeouts, rate limiting, bulkheading, and fallback.
//
// It is a separate module (go.arpabet.com/value-rpc/resilience) so these policies
// — and any dependencies they grow — never burden a plain client. Core ships only
// the interceptor seam; the policy lives here. Install policies with
// valueclient.WithInterceptors:
//
//	cli := valueclient.NewClient(addr, "", valueclient.WithInterceptors(
//		resilience.RateLimit(100, 20),                 // outermost
//		resilience.CircuitBreaker(),
//		resilience.Retry(resilience.WithMaxAttempts(3)),
//		resilience.Timeout(2*time.Second),             // innermost (per attempt)
//	))
//
// Order matters: the first interceptor is outermost. A typical chain is rate
// limit → circuit breaker → retry → timeout → call, so the breaker observes whole
// (post-retry) operations and the timeout bounds each individual attempt.
//
// Policies classify outcomes with the wire-agnostic error codes from core
// (valuerpc.CodeOf): by default Retry retries only CodeUnavailable and
// CodeResourceExhausted — codes that mean the server did not process the request,
// so retrying is safe even for non-idempotent calls. Widening the retryable set
// (e.g. to include codes that may have executed) makes retry at-least-once; only
// do so for idempotent methods (see WithRetryable).
package resilience
