/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"net"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// Handler signatures receive a context.Context as their first argument. The
// context is cancelled when the connection drops, the server shuts down, the
// request is cancelled (client CancelRequest), or the client's SLA deadline
// elapses — handlers should honour it for cancellation and deadline propagation
// to downstream work.
type Function func(ctx context.Context, args value.Value) (value.Value, error)
type OutgoingStream func(ctx context.Context, args value.Value) (<-chan value.Value, error)
type IncomingStream func(ctx context.Context, args value.Value, inC <-chan value.Value) error
type Chat func(ctx context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error)

// ConnectAuthorizer is called once per new connection, before the handshake. If
// it returns an error the connection is rejected and closed. Combine it with
// valuerpc.PeerCredOf for Unix-domain-socket peer authorization.
type ConnectAuthorizer func(conn valuerpc.MsgConn) error

// Authenticator is called during the handshake (on first connect and on every
// reconnect) with the credential the client attached via Client.SetCredential
// (value.Null when none was sent). Returning an error rejects the connection.
// This is the transport-agnostic auth hook for credential schemes the
// connect-authorizer cannot reach — bearer tokens, API keys, HMAC — that travel
// in the handshake rather than in TLS certs or Unix peer credentials.
type Authenticator func(conn valuerpc.MsgConn, credential value.Value) error

type Server interface {
	AddFunction(name string, args valuerpc.TypeDef, res valuerpc.TypeDef, cb Function) error

	// GET for client
	AddOutgoingStream(name string, args valuerpc.TypeDef, cb OutgoingStream) error

	// PUT for client
	AddIncomingStream(name string, args valuerpc.TypeDef, cb IncomingStream) error

	// Dual channel chat
	AddChat(name string, args valuerpc.TypeDef, cb Chat) error

	// Addr returns the address the server is listening on. Useful when binding
	// to an ephemeral port (":0").
	Addr() net.Addr

	// SetConnectAuthorizer installs a per-connection authorization hook (called
	// before the handshake). Call before Run. Passing nil clears it.
	SetConnectAuthorizer(fn ConnectAuthorizer)

	// SetAuthenticator installs a handshake authentication hook that validates
	// the client's credential (see Client.SetCredential). Call before Run.
	SetAuthenticator(fn Authenticator)

	Run() error

	Close() error
}
