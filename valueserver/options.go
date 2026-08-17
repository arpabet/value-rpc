/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"time"

	"go.arpabet.com/value-rpc/valuerpc"
)

// serverConfig is the resolved per-server configuration. It is seeded from the
// package-level defaults (MaxConnections, MaxConcurrentRequests, …) and then
// overridden by the ServerOptions passed to a constructor. Capturing it at
// construction means two servers in one process can be tuned independently and
// the values are never read from a mutable global at runtime.
type serverConfig struct {
	// name labels this server's connection log lines ("webmail-ws",
	// "vrpc-server") — a process running several servers is unreadable when
	// every line just says "new connection". Empty = the listener address.
	name                  string
	maxConnections        int64
	maxConcurrentRequests int64
	maxConcurrentStreams  int64
	outgoingQueueCap      int
	incomingQueueCap      int
	maxPending            int
	handshakeTimeout      time.Duration
	metrics               valuerpc.Metrics
	metadataExtractor     func(ctx context.Context, md valuerpc.Metadata) context.Context

	// transport-level (used when a convenience constructor builds the listener)
	keepAlive    time.Duration
	writeTimeout time.Duration
	maxFrameSize int

	// WebSocket-only (used when a convenience constructor builds a WS listener)
	wsCodecs      []valuerpc.WireCodec
	wsOrigins     []string
	wsCompression bool

	// session lifecycle
	sessionRetention time.Duration
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
		metrics:               valuerpc.NopMetrics{},
		keepAlive:             KeepAlivePeriod,
		writeTimeout:          DefaultTimeout,
		maxFrameSize:          valuerpc.MaxFrameSize,
		sessionRetention:      SessionRetention,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.metrics == nil {
		cfg.metrics = valuerpc.NopMetrics{}
	}
	return cfg
}

// wsOptions translates the WebSocket-specific server settings into the
// valuerpc.WSOption list consumed by NewWebSocketListener / NewWebSocketHandler.
func (c serverConfig) wsOptions() []valuerpc.WSOption {
	var opts []valuerpc.WSOption
	if len(c.wsCodecs) > 0 {
		opts = append(opts, valuerpc.WithWSCodecs(c.wsCodecs...))
	}
	if len(c.wsOrigins) > 0 {
		opts = append(opts, valuerpc.WithWSOriginPatterns(c.wsOrigins...))
	}
	if c.wsCompression {
		opts = append(opts, valuerpc.WithWSCompression())
	}
	return opts
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

// WithMetrics installs a metrics sink for server-side request/error counters, the
// in-flight gauge, and stream throughput (the same valuerpc.Metrics interface as
// the client). Without it metrics are a no-op.
func WithMetrics(m valuerpc.Metrics) ServerOption {
	return func(c *serverConfig) { c.metrics = m }
}

// WithMetadataExtractor enriches each handler's context from the request's
// incoming metadata before the handler runs — the server-side seam for
// distributed tracing. The raw metadata is always available via
// valuerpc.MetadataFromContext; this hook lets a propagator turn it into a real
// context (e.g. an OpenTelemetry span context):
//
//	WithMetadataExtractor(func(ctx context.Context, md valuerpc.Metadata) context.Context {
//		return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(md))
//	})
func WithMetadataExtractor(extract func(ctx context.Context, md valuerpc.Metadata) context.Context) ServerOption {
	return func(c *serverConfig) { c.metadataExtractor = extract }
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

// WithWireCodecs restricts the WebSocket wire codecs the server negotiates, in
// preference order (valuerpc.MsgpackWireCodec, valuerpc.JsonWireCodec). The
// default (unset) enables both — msgpack for a client that offers no
// subprotocol (every existing peer), JSON for one offering "vrpc.json". Pass a
// single codec to force a JSON-only or msgpack-only endpoint. Only affects
// WebSocket servers built by a convenience constructor.
func WithWireCodecs(codecs ...valuerpc.WireCodec) ServerOption {
	return func(c *serverConfig) { c.wsCodecs = codecs }
}

// WithWebSocketOrigins sets the browser Origins allowed to open a WebSocket
// (CSRF policy). The default rejects cross-origin requests (Origin must match
// Host); set patterns like "app.example.com" to allow an api-subdomain split,
// or "*" to allow any origin. Only affects WebSocket servers built by a
// convenience constructor.
// ServerName labels this server's connection log lines ("webmail-ws",
// "vrpc-server"), so a process running several servers says WHICH listener a
// connection belongs to. Unset, the listener's address is used.
func ServerName(name string) ServerOption {
	return func(cfg *serverConfig) { cfg.name = name }
}

func WithWebSocketOrigins(patterns ...string) ServerOption {
	return func(c *serverConfig) { c.wsOrigins = patterns }
}

// WithWebSocketCompression enables permessage-deflate, shrinking JSON frames to
// roughly msgpack size for typical payloads. Off by default. Only affects
// WebSocket servers built by a convenience constructor.
func WithWebSocketCompression() ServerOption {
	return func(c *serverConfig) { c.wsCompression = true }
}

// WithSessionRetention sets how long a disconnected client's session (its
// serving state, so a reconnect can resume it) is retained before it is reaped.
// 0 uses the SessionRetention default; a negative duration disables reaping
// (the pre-reaping behavior — sessions live until server shutdown). Reaping
// bounds the goroutines/memory a churn of short-lived clients (e.g. browser
// tabs) can accumulate.
func WithSessionRetention(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.sessionRetention = d }
}
