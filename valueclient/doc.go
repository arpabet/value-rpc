/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Package valueclient is the vRPC client.
//
// A Client dials a server over any [go.arpabet.com/value-rpc/valuerpc] transport
// (TCP, Unix socket, WebSocket, TLS/mTLS, in-memory, or a custom dialer) and
// invokes remote functions by name with the four call patterns, all multiplexed
// over a single connection:
//
//   - CallFunction — unary;
//   - GetStream    — server-streaming;
//   - PutStream    — client-streaming;
//   - Chat         — bidirectional.
//
// Every call takes a context.Context: a deadline is sent to the server as the
// request SLA, and cancellation tears the request (or stream) down. The client
// also provides context-aware bounded dialing (ConnectContext, WithDialTimeout),
// a configurable reconnect policy (fail-fast, idempotent replay, and exponential
// backoff — see ReconnectPolicy), pluggable structured logging (WithLogger) and
// metrics (WithMetrics), per-request metadata injection for distributed tracing
// (WithMetadata), credit-based flow control, and optional statically-typed call
// sites via CallUnary together with [go.arpabet.com/value-rpc/valuerpc.Codec].
//
// Construct a client with NewClient (scheme-aware address) or a transport-
// specific constructor (NewUnixClient, NewWebSocketClient, NewTLSClient,
// NewMemClient, NewClientWithDialer) and tune it with functional ClientOptions.
package valueclient
