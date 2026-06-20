/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"net"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// The handler types are aliases of the shared valuerpc handler types, so the
// registration surface (valuerpc.Registrar) is literally identical on the server
// and the client. Each handler receives a context.Context cancelled when the
// connection drops, the server shuts down, the request is cancelled (client
// CancelRequest), or the client's SLA deadline elapses — honour it for
// cancellation and deadline propagation to downstream work.
type Function = valuerpc.Function
type OutgoingStream = valuerpc.OutgoingStream
type IncomingStream = valuerpc.IncomingStream
type Chat = valuerpc.Chat

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
//
// The returned principal is the authenticated identity (e.g. a user id or cert
// subject; "" for anonymous). Session resumption is bound to it: a reconnect that
// presents a valid session token but authenticates as a different principal is
// rejected, so a leaked token alone cannot let one principal take over another's
// session.
type Authenticator func(conn valuerpc.MsgConn, credential value.Value) (principal string, err error)

type Server interface {
	// Registrar is the shared handler-registration surface (AddFunction,
	// AddOutgoingStream, AddIncomingStream, AddChat) — identical to
	// valueclient.Client's, so both sides of a connection register handlers the
	// same way.
	valuerpc.Registrar

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
