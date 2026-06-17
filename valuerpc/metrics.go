/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import "time"

// Metrics receives RPC observability events. Implement it to feed Prometheus,
// OpenTelemetry, statsd, etc., and pass it via WithMetrics (client and/or
// server). Every method must be cheap and non-blocking; they may be called
// concurrently from many goroutines. The no-op default (NopMetrics) costs
// nothing.
//
// Derived signals:
//   - request counter: count of RequestEnd
//   - error counter: count of RequestEnd with code != CodeOK (group by code)
//   - in-flight gauge: running (RequestBegin − RequestEnd)
//   - latency: RequestEnd elapsed (histogram by method/code)
//   - stream throughput: rate of StreamValue
//   - reconnects: count of Reconnect
type Metrics interface {
	// RequestBegin marks a request (unary call or stream) entering flight.
	RequestBegin(method string)
	// RequestEnd marks it leaving flight: code is the outcome (CodeOK on success),
	// elapsed the wall-clock duration.
	RequestEnd(method string, code Code, elapsed time.Duration)
	// StreamValue records one stream value transferred (sent or received).
	StreamValue(method string)
	// Reconnect records a client reconnect attempt.
	Reconnect()
}

// NopMetrics is a Metrics that does nothing; it is the default.
type NopMetrics struct{}

func (NopMetrics) RequestBegin(string)                    {}
func (NopMetrics) RequestEnd(string, Code, time.Duration) {}
func (NopMetrics) StreamValue(string)                     {}
func (NopMetrics) Reconnect()                             {}
