/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"go.arpabet.com/value-rpc/valuerpc"
)

// clientConfig is the resolved per-client configuration, seeded from the
// package-level defaults and overridden by ClientOptions at construction.
type clientConfig struct {
	sendingCap int64
	timeoutMls int64
	maxPending int
}

func newClientConfig(opts []ClientOption) clientConfig {
	cfg := clientConfig{
		sendingCap: DefaultSendingCap,
		timeoutMls: DefaultTimeoutMls,
		maxPending: valuerpc.DefaultMaxPending,
	}
	for _, o := range opts {
		o(&cfg)
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
