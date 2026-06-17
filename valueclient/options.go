/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
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

	// transport-level (used when a convenience constructor builds the dialer)
	keepAlive    time.Duration
	writeTimeout time.Duration
	maxFrameSize int
}

func newClientConfig(opts []ClientOption) clientConfig {
	cfg := clientConfig{
		sendingCap:   DefaultSendingCap,
		timeoutMls:   DefaultTimeoutMls,
		maxPending:   valuerpc.DefaultMaxPending,
		logger:       zap.NewNop(),
		keepAlive:    KeepAlivePeriod,
		writeTimeout: DefaultTimeout,
		maxFrameSize: valuerpc.MaxFrameSize,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = zap.NewNop()
	}
	return cfg
}

// ClientOption overrides a single per-client setting. Pass options to any client
// constructor (NewClient, NewClientWithDialer, …).
type ClientOption func(*clientConfig)

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

// WithLogger sets the structured logger the client uses for connection,
// reconnect, and protocol diagnostics (the same *zap.Logger glue applications
// pass to valueserver). Without it the client is silent (a no-op logger). A nil
// logger is treated as the no-op logger.
func WithLogger(logger *zap.Logger) ClientOption {
	return func(c *clientConfig) { c.logger = logger }
}
