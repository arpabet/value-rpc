/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"time"

	"go.arpabet.com/value-rpc/valuerpc"
)

// serverConfig is the resolved per-server configuration. It is seeded from the
// package-level defaults (MaxConnections, MaxConcurrentRequests, …) and then
// overridden by the ServerOptions passed to a constructor. Capturing it at
// construction means two servers in one process can be tuned independently and
// the values are never read from a mutable global at runtime.
type serverConfig struct {
	maxConnections        int64
	maxConcurrentRequests int64
	maxConcurrentStreams  int64
	outgoingQueueCap      int
	incomingQueueCap      int
	maxPending            int
	handshakeTimeout      time.Duration

	// transport-level (used when a convenience constructor builds the listener)
	keepAlive    time.Duration
	writeTimeout time.Duration
	maxFrameSize int
}

func newServerConfig(opts []ServerOption) serverConfig {
	cfg := serverConfig{
		maxConnections:        MaxConnections,
		maxConcurrentRequests: MaxConcurrentRequests,
		maxConcurrentStreams:  MaxConcurrentStreams,
		outgoingQueueCap:      OutgoingQueueCap,
		incomingQueueCap:      IncomingQueueCap,
		maxPending:            valuerpc.DefaultMaxPending,
		handshakeTimeout:      HandshakeTimeout,
		keepAlive:             KeepAlivePeriod,
		writeTimeout:          DefaultTimeout,
		maxFrameSize:          valuerpc.MaxFrameSize,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// ServerOption overrides a single per-server setting. Pass options to any server
// constructor (NewServer, NewServerWithListener, …). Each option mirrors a
// package-level default of the same name, so unset options keep the default.
type ServerOption func(*serverConfig)

// WithMaxConnections caps simultaneously open connections (0 = unlimited).
func WithMaxConnections(n int64) ServerOption {
	return func(c *serverConfig) { c.maxConnections = n }
}

// WithMaxConcurrentRequests caps concurrent request handlers per connection
// (0 = unlimited); over the limit a request is rejected with an error.
func WithMaxConcurrentRequests(n int64) ServerOption {
	return func(c *serverConfig) { c.maxConcurrentRequests = n }
}

// WithMaxConcurrentStreams caps open streams per connection (0 = unlimited).
func WithMaxConcurrentStreams(n int64) ServerOption {
	return func(c *serverConfig) { c.maxConcurrentStreams = n }
}

// WithOutgoingQueueCap sets the per-client server send-buffer size.
func WithOutgoingQueueCap(n int) ServerOption {
	return func(c *serverConfig) { c.outgoingQueueCap = n }
}

// WithIncomingQueueCap sets the per-request inbound stream buffer size.
func WithIncomingQueueCap(n int) ServerOption {
	return func(c *serverConfig) { c.incomingQueueCap = n }
}

// WithStreamMaxPending sets the per-stream pending-queue bound before a consumer
// that ignores flow control is failed.
func WithStreamMaxPending(n int) ServerOption {
	return func(c *serverConfig) { c.maxPending = n }
}

// WithHandshakeTimeout bounds how long a new connection has to complete the
// handshake (0 = no limit).
func WithHandshakeTimeout(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.handshakeTimeout = d }
}

// WithKeepAlivePeriod sets TCP keepalive (and the WebSocket ping interval) for
// connections built by a convenience constructor. 0 disables. No effect when you
// supply your own listener via NewServerWithListener.
func WithKeepAlivePeriod(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.keepAlive = d }
}

// WithWriteTimeout bounds each message write on connections built by a
// convenience constructor. No effect with NewServerWithListener.
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.writeTimeout = d }
}

// WithMaxFrameSize bounds inbound message size on connections built by a
// convenience constructor (> 0 enforces, <= 0 unlimited). No effect with
// NewServerWithListener — set the limit when you build your own listener.
func WithMaxFrameSize(n int) ServerOption {
	return func(c *serverConfig) { c.maxFrameSize = n }
}
