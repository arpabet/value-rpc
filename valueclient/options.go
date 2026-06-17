/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"time"

	"go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
)

// clientConfig is the resolved per-client configuration, seeded from the
// package-level defaults and overridden by ClientOptions at construction.
type clientConfig struct {
	sendingCap int64
	timeoutMls int64
	maxPending int
	logger     *zap.Logger
	metrics    valuerpc.Metrics
	metadata   func(context.Context) valuerpc.Metadata
	reconnect  ReconnectPolicy

	// transport-level (used when a convenience constructor builds the dialer)
	keepAlive    time.Duration
	writeTimeout time.Duration
	maxFrameSize int
	dialTimeout  time.Duration
}

// DefaultDialTimeout bounds a connection dial when the connect context carries no
// deadline, so Connect cannot block indefinitely on an unreachable peer.
var DefaultDialTimeout = 30 * time.Second

func newClientConfig(opts []ClientOption) clientConfig {
	cfg := clientConfig{
		sendingCap:   DefaultSendingCap,
		timeoutMls:   DefaultTimeoutMls,
		maxPending:   valuerpc.DefaultMaxPending,
		logger:       zap.NewNop(),
		metrics:      valuerpc.NopMetrics{},
		keepAlive:    KeepAlivePeriod,
		writeTimeout: DefaultTimeout,
		maxFrameSize: valuerpc.MaxFrameSize,
		dialTimeout:  DefaultDialTimeout,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = zap.NewNop()
	}
	if cfg.metrics == nil {
		cfg.metrics = valuerpc.NopMetrics{}
	}
	return cfg
}

// ReconnectPolicy governs what happens to in-flight requests when the connection
// drops and is re-established. The zero value is fail-fast: every in-flight
// request (unary and streams) is failed immediately with ErrConnectionLost
// (CodeUnavailable) instead of hanging until its timeout.
type ReconnectPolicy struct {
	// ReplayUnary, when non-nil, reports whether an in-flight unary call to method
	// is safe to re-send on the new connection (i.e. idempotent). Matching calls
	// are replayed, keeping their original deadline budget; everything else —
	// streams and non-idempotent unary calls, which cannot be safely resumed — is
	// still failed fast. Replay at-least-once: the server may have already run the
	// call before the drop, so only opt in methods with no observable effect on
	// repetition.
	ReplayUnary func(method string) bool
}

// ClientOption overrides a single per-client setting. Pass options to any client
// constructor (NewClient, NewClientWithDialer, …).
type ClientOption func(*clientConfig)

// WithReconnectPolicy sets how in-flight requests are handled across a reconnect.
// The default is fail-fast (see ReconnectPolicy).
func WithReconnectPolicy(p ReconnectPolicy) ClientOption {
	return func(c *clientConfig) { c.reconnect = p }
}

// WithSendingCap sets the outbound request-queue size.
func WithSendingCap(n int64) ClientOption {
	return func(c *clientConfig) { c.sendingCap = n }
}

// WithTimeout sets the initial default call timeout in milliseconds (equivalent
// to calling SetTimeout right after construction).
func WithTimeout(timeoutMls int64) ClientOption {
	return func(c *clientConfig) { c.timeoutMls = timeoutMls }
}

// WithStreamMaxPending sets the per-stream receive pending-queue bound before a
// slow local consumer is failed.
func WithStreamMaxPending(n int) ClientOption {
	return func(c *clientConfig) { c.maxPending = n }
}

// WithKeepAlivePeriod sets TCP keepalive (and the WebSocket ping interval) for a
// dialer built by a convenience constructor. 0 disables. No effect with
// NewClientWithDialer.
func WithKeepAlivePeriod(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.keepAlive = d }
}

// WithWriteTimeout bounds each message write for a dialer built by a convenience
// constructor. No effect with NewClientWithDialer.
func WithWriteTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.writeTimeout = d }
}

// WithMaxFrameSize bounds inbound message size for a dialer built by a
// convenience constructor (> 0 enforces, <= 0 unlimited). No effect with
// NewClientWithDialer.
func WithMaxFrameSize(n int) ClientOption {
	return func(c *clientConfig) { c.maxFrameSize = n }
}

// WithDialTimeout bounds a connection dial when ConnectContext (or Connect) is
// called with a context that carries no deadline. 0 disables the bound. Ignored
// when the connect context already has a deadline.
func WithDialTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.dialTimeout = d }
}

// WithMetrics installs a metrics sink for request/error/reconnect counters, the
// in-flight gauge, and stream throughput. Without it metrics are a no-op.
func WithMetrics(m valuerpc.Metrics) ClientOption {
	return func(c *clientConfig) { c.metrics = m }
}

// WithMetadata installs an injector that produces per-request metadata from the
// call's context (string key/value pairs added to the request envelope and
// surfaced on the server handler's context). This is the seam for distributed
// tracing; with OpenTelemetry:
//
//	WithMetadata(func(ctx context.Context) valuerpc.Metadata {
//		c := propagation.MapCarrier{}
//		otel.GetTextMapPropagator().Inject(ctx, c)
//		return c
//	})
func WithMetadata(inject func(ctx context.Context) valuerpc.Metadata) ClientOption {
	return func(c *clientConfig) { c.metadata = inject }
}

// WithLogger sets the structured logger the client uses for connection,
// reconnect, and protocol diagnostics (the same *zap.Logger glue applications
// pass to valueserver). Without it the client is silent (a no-op logger). A nil
// logger is treated as the no-op logger.
func WithLogger(logger *zap.Logger) ClientOption {
	return func(c *clientConfig) { c.logger = logger }
}
