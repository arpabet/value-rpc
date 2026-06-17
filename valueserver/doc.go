/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Package valueserver is the vRPC server.
//
// Register Go functions by name and serve them over any
// [go.arpabet.com/value-rpc/valuerpc] transport:
//
//   - AddFunction       — unary;
//   - AddOutgoingStream — server-streaming;
//   - AddIncomingStream — client-streaming;
//   - AddChat           — bidirectional.
//
// Handlers receive a context.Context that carries the caller's deadline,
// cancellation, and request metadata (valuerpc.MetadataFromContext), so they can
// propagate trace context and abandon work when the client goes away.
//
// The server is hardened for production: DoS caps (WithMaxConnections,
// WithMaxConcurrentRequests, WithMaxConcurrentStreams), credit-based flow control
// that is lossless and never head-of-line blocks, a handshake Authenticator with
// principal-bound session resumption, a connect authorizer for transport-level
// identity (Unix peer credentials via valuerpc.PeerCredOf, or mTLS client certs
// via valuerpc.PeerCertificates), pluggable metrics (WithMetrics), and typed
// handler registration via AddUnary together with
// [go.arpabet.com/value-rpc/valuerpc.Codec].
//
// Construct a server with NewServer (scheme-aware address) or a transport-
// specific constructor (NewUnixServer, NewWebSocketServer/NewWebSocketHandler,
// NewTLSServer, NewMemServer, NewServerWithListener) and tune it with functional
// ServerOptions.
package valueserver
